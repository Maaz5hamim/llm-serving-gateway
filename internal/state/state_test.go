package state

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDistributedLimitAndBudget(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("integration database not configured")
	}
	ctx := context.Background()
	s, e := Open(ctx, os.Getenv("TEST_REDIS_URL"), os.Getenv("TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	k, raw, e := s.Create(ctx, Key{Name: "integration", RPM: 10, DailyTokens: 100, DailyMicros: 100, TTL: 60})
	if e != nil {
		t.Fatal(e)
	}
	defer s.DB.Exec(ctx, "DELETE FROM api_keys WHERE id=$1", k.ID)
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _, _, e := s.Limit(ctx, k)
			if e != nil {
				t.Error(e)
			}
			if ok {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 10 {
		t.Fatal(accepted.Load())
	}
	bk := BudgetKey(k.ID)
	defer s.Redis.Del(ctx, bk, "rate:"+k.ID)
	ok, left, e := s.Reserve(ctx, k, bk, 80, 80)
	if e != nil || !ok || left != 20 {
		t.Fatal(ok, left, e)
	}
	ok, _, _ = s.Reserve(ctx, k, bk, 21, 0)
	if ok {
		t.Fatal("overspent tokens")
	}
	ok, _, _ = s.Reserve(ctx, k, bk, 1, 21)
	if ok {
		t.Fatal("overspent cost")
	}
	if e = s.Settle(ctx, bk, -60, -60); e != nil {
		t.Fatal(e)
	}
	ok, left, e = s.Reserve(ctx, k, bk, 80, 80)
	if e != nil || !ok || left != 0 {
		t.Fatal(ok, left, e)
	}
	s.Put(ctx, "test:"+k.ID, map[string]int{"v": 1}, time.Second)
	var v map[string]int
	if !s.Get(ctx, "test:"+k.ID, &v) || v["v"] != 1 {
		t.Fatal("cache round trip")
	}
	s.Redis.Del(ctx, "test:"+k.ID)
	if _, e = s.Lookup(ctx, raw); e != nil {
		t.Fatal(e)
	}
	if e = s.Revoke(ctx, k.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Lookup(ctx, raw); e == nil {
		t.Fatal("revoked key accepted")
	}
}
