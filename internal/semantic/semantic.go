// Package semantic provides an embedding-based cache layer. It embeds a prompt,
// searches previously stored embeddings by cosine similarity, and returns a
// matching exact-cache key when one is found above the configured threshold.
// Embeddings are stored in the semantic_cache PostgreSQL table as JSONB.
// For large deployments, replace the table scan with pgvector or a dedicated
// vector store.
package semantic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config configures the semantic cache. URL, Model, and KeyEnv select the
// embedding provider (any OpenAI-compatible /v1/embeddings endpoint).
type Config struct {
	URL       string        // e.g. "https://api.openai.com/v1/embeddings"
	Model     string        // e.g. "text-embedding-3-small"
	KeyEnv    string        // env var that holds the API key
	Threshold float64       // cosine similarity threshold; default 0.95
	MaxAge    time.Duration // max age of stored embeddings; default 7 days
}

// Cache is the semantic cache client. Use New to construct one.
type Cache struct {
	cfg    Config
	db     *pgxpool.Pool
	client *http.Client
}

// New constructs a Cache. db may be nil, in which case Store and Lookup are no-ops.
func New(cfg Config, db *pgxpool.Pool) *Cache {
	if cfg.Threshold <= 0 {
		cfg.Threshold = 0.95
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = 7 * 24 * time.Hour
	}
	return &Cache{cfg: cfg, db: db, client: &http.Client{Timeout: 10 * time.Second}}
}

// Embed calls the configured embedding API and returns the vector for text.
func (c *Cache) Embed(ctx context.Context, text string) ([]float64, error) {
	payload, _ := json.Marshal(map[string]any{"input": text, "model": c.cfg.Model})
	req, err := http.NewRequestWithContext(ctx, "POST", c.cfg.URL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if key := os.Getenv(c.cfg.KeyEnv); key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("embedding API status %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var v struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(b, &v); err != nil || len(v.Data) == 0 {
		return nil, fmt.Errorf("invalid embedding response")
	}
	return v.Data[0].Embedding, nil
}

// Lookup finds the stored embedding with the highest cosine similarity to
// embedding that also exceeds the configured threshold. It returns the
// matching exact-cache key, or ("", false) when none qualifies.
func (c *Cache) Lookup(ctx context.Context, apiKeyID, model string, embedding []float64) (string, bool) {
	if c.db == nil {
		return "", false
	}
	rows, err := c.db.Query(ctx,
		`SELECT cache_key, embedding FROM semantic_cache WHERE api_key_id=$1 AND model_alias=$2 AND created_at > $3`,
		apiKeyID, model, time.Now().Add(-c.cfg.MaxAge))
	if err != nil {
		return "", false
	}
	defer rows.Close()

	bestKey := ""
	bestSim := c.cfg.Threshold - 1e-9
	for rows.Next() {
		var cacheKey string
		var embJSON []byte
		if err := rows.Scan(&cacheKey, &embJSON); err != nil {
			continue
		}
		var stored []float64
		if err := json.Unmarshal(embJSON, &stored); err != nil {
			continue
		}
		if sim := cosine(embedding, stored); sim > bestSim {
			bestSim = sim
			bestKey = cacheKey
		}
	}
	return bestKey, bestKey != ""
}

// Store saves an embedding so future Lookup calls can find it.
func (c *Cache) Store(ctx context.Context, apiKeyID, model, cacheKey string, embedding []float64) {
	if c.db == nil {
		return
	}
	b, err := json.Marshal(embedding)
	if err != nil {
		return
	}
	c.db.Exec(ctx,
		`INSERT INTO semantic_cache(api_key_id,model_alias,cache_key,embedding) VALUES($1,$2,$3,$4)`,
		apiKeyID, model, cacheKey, b)
}

// ExtractText returns a plain-text representation of an OpenAI-style messages
// array suitable for embedding. It concatenates role: content pairs.
func ExtractText(messages []any) string {
	var buf bytes.Buffer
	for _, m := range messages {
		msg, ok := m.(map[string]any)
		if !ok {
			continue
		}
		role, _ := msg["role"].(string)
		content, _ := msg["content"].(string)
		if content != "" {
			if buf.Len() > 0 {
				buf.WriteByte('\n')
			}
			buf.WriteString(role)
			buf.WriteString(": ")
			buf.WriteString(content)
		}
	}
	return buf.String()
}

func cosine(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}
