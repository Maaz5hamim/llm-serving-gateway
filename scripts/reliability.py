#!/usr/bin/env python3
"""Free local mock chaos checks. Targets only the explicitly named Compose project."""
import concurrent.futures,json,os,pathlib,subprocess,time,urllib.request,urllib.error,uuid
base=os.getenv('BASE_URL','http://localhost:8080');admin=os.environ['ADMIN_API_KEY'];project=os.environ['COMPOSE_PROJECT_NAME'];root=pathlib.Path(__file__).resolve().parents[1]
def api(path,body=None,key=admin,method=None,url=None):
 r=urllib.request.Request((url or base)+path,data=None if body is None else json.dumps(body).encode(),headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'},method=method)
 try:
  with urllib.request.urlopen(r,timeout=8) as res:return res.status,dict(res.headers),res.read()
 except urllib.error.HTTPError as e:return e.code,dict(e.headers),e.read()
def mock(rate):subprocess.run(['docker','exec',project+'-gateway-1','wget','-qO-','--header=Content-Type: application/json','--post-data='+json.dumps({'error_rate':rate,'latency_ms':200}),'http://mock-primary:8081/config'],check=True,capture_output=True)
def tox(body):return api('/proxies/primary',body,url='http://localhost:8474')
code,_,b=api('/admin/keys',{'name':'reliability','rpm_limit':10000,'daily_tokens':100000000,'daily_cost_micros':100000000});assert code==201;b=json.loads(b);key=b['key'];report={};payload={'model':'fast-chat','messages':[{'role':'user','content':'chaos-'+str(uuid.uuid4())}],'max_tokens':32}
def one(_):
 r=urllib.request.Request(base+'/v1/chat/completions',data=json.dumps(payload).encode(),headers={'Authorization':'Bearer '+key,'Content-Type':'application/json','X-Cache':'bypass'});start=time.monotonic()
 try:
  with urllib.request.urlopen(r,timeout=8) as res:res.read();return res.status,res.headers.get('X-Provider'),time.monotonic()-start
 except urllib.error.HTTPError as e:return e.code,None,time.monotonic()-start
 except Exception:return 0,None,time.monotonic()-start
def load(seconds):
 start=time.monotonic();results=[]
 with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
  while time.monotonic()-start<seconds:
   results.extend(pool.map(one,range(10)))
 return {'requests':len(results),'client_error_rate':sum(x[0]!=200 for x in results)/len(results),'backup_responses':sum(x[1]=='mock-backup' for x in results),'primary_responses':sum(x[1]=='mock-primary' for x in results),'max_latency_s':max(x[2] for x in results)}
try:
 mock(.5);report['primary_50pct_errors']=load(10);print(json.dumps(report),flush=True);mock(0)
 # Recreating the gateway resets process-local circuits for an independent outage test.
 subprocess.run(['docker','restart',project+'-gateway-1'],check=True,capture_output=True);time.sleep(2)
 tox({'enabled':False});start=time.monotonic();first=one(0);report['first_outage_response']={'status':first[0],'provider':first[1],'latency_s':first[2]};print(json.dumps(report['first_outage_response']),flush=True)
 for i in range(4):report[f'outage_{i*15}_{(i+1)*15}s']=load(15);print(json.dumps(report[f'outage_{i*15}_{(i+1)*15}s']),flush=True)
 tox({'enabled':True});report['recovery']=load(35);print(json.dumps(report['recovery']),flush=True)
 # Simulate a slow primary via Toxiproxy.
 api('/proxies/primary/toxics',{'name':'reliability-slow','type':'latency','stream':'downstream','attributes':{'latency':5000,'jitter':0}},url='http://localhost:8474');report['latency_5s']=load(10);print(json.dumps(report['latency_5s']),flush=True)
 api('/proxies/primary/toxics/reliability-slow',{},method='DELETE',url='http://localhost:8474')
 subprocess.run(['docker','restart',project+'-redis-1'],check=True,capture_output=True);time.sleep(2);report['redis_recovered_status']=one(0)[0]
 time.sleep(2);report['usage']=json.loads(api('/admin/usage?key='+b['limits']['id'])[2]);(root/'docs/results/reliability.json').write_text(json.dumps(report,indent=2));assert all(v['client_error_rate']==0 for v in report.values() if isinstance(v,dict) and 'client_error_rate' in v),report
finally:
 tox({'enabled':True});api('/proxies/primary/toxics/reliability-slow',{},method='DELETE',url='http://localhost:8474');mock(0);api('/admin/keys/'+b['limits']['id'],method='DELETE')
