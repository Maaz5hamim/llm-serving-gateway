#!/usr/bin/env python3
"""Short synthetic local benchmark; not a production capacity certification."""
import json,os,pathlib,subprocess,time,urllib.request,urllib.error
root=pathlib.Path(__file__).resolve().parents[1];base=os.getenv('BASE_URL','http://localhost:8080');admin=os.environ['ADMIN_API_KEY']
def request(path,body=None,key=admin,method=None):
 r=urllib.request.Request(base+path,data=None if body is None else json.dumps(body).encode(),headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'},method=method)
 with urllib.request.urlopen(r,timeout=20) as res:return res.read()
k=json.loads(request('/admin/keys',{'name':'synthetic-benchmark','rpm_limit':1000000,'daily_tokens':1000000000,'daily_cost_micros':1000000000}));env={**os.environ,'API_KEY':k['key']};report=[]
try:
 for mode,rate in [('overhead',50),('overhead',200),('overhead',500),('cache',500)]:
  before=json.loads(request('/admin/usage?key='+k['limits']['id']));name=f'{mode}-{rate}';summary=f'docs/results/{name}.json';args=['docker','run','--rm','-v',str(root)+':/work','-w','/work/loadtest','-e','API_KEY','-e','MODE='+mode,'-e','RATE='+str(rate),'-e','DURATION=15s','grafana/k6:0.55.0','run','--summary-export','/work/'+summary,'workload.js']
  with open(root/'docs/results'/f'{name}.log','w') as log:result=subprocess.run(args,env=env,stdout=log,stderr=subprocess.STDOUT)
  if not (root/summary).exists():raise RuntimeError('k6 failed: see '+name+'.log')
  time.sleep(2);after=json.loads(request('/admin/usage?key='+k['limits']['id']));spent=after['cost_usd']-before['cost_usd'];saved=after['saved_usd']-before['saved_usd'];data=json.loads((root/summary).read_text());m=data['metrics'];report.append({'mode':mode,'cost_saved_pct':100*saved/(spent+saved) if spent+saved else 0,'dropped_iterations':m.get('dropped_iterations',{}).get('count',0),'offered_rps':rate,'exit_code':result.returncode,'requests':m['http_reqs']['count'],'achieved_rps':m['http_reqs']['rate'],'failed_rate':m['http_req_failed']['value'],'overhead_p99_ms':m['gateway_overhead_ms'].get('p(99)'),'duration_p95_ms':m['http_req_duration'].get('p(95)')});print(json.dumps(report[-1]),flush=True)
 time.sleep(2);usage=json.loads(request('/admin/usage?key='+k['limits']['id']));(root/'docs/results/usage.json').write_text(json.dumps(usage,indent=2));(root/'docs/results/summary.json').write_text(json.dumps(report,indent=2))
finally:request('/admin/keys/'+k['limits']['id'],method='DELETE')
