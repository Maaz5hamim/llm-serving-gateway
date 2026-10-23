#!/usr/bin/env python3
"""Verify bounded database buffering and fail-closed Redis behavior in a local test stack."""
import concurrent.futures,json,os,pathlib,subprocess,time,urllib.request,urllib.error
project=os.environ['COMPOSE_PROJECT_NAME'];admin=os.environ['ADMIN_API_KEY'];base=os.getenv('BASE_URL','http://localhost:8080')
def req(path,body=None,key=admin,method=None):
 r=urllib.request.Request(base+path,data=None if body is None else json.dumps(body).encode(),headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'},method=method)
 try:
  with urllib.request.urlopen(r,timeout=10) as res:return res.status,res.read()
 except urllib.error.HTTPError as e:return e.code,e.read()
k=json.loads(req('/admin/keys',{'name':'storage-outage','rpm_limit':10000,'daily_tokens':10000000})[1]);payload={'model':'fast-chat','messages':[{'role':'user','content':'storage outage'}],'max_tokens':32};report={}
def call(_):return req('/v1/chat/completions',payload,k['key'])[0]
def docker(*args):return subprocess.run(['docker',*args],check=True,capture_output=True)
try:
 assert call(0)==200
 docker('pause',project+'-postgres-1')
 try:
  with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:statuses=list(pool.map(call,range(30)))
  report['postgres_paused_statuses']={str(s):statuses.count(s) for s in set(statuses)};assert all(s==200 for s in statuses),statuses
 finally:docker('unpause',project+'-postgres-1')
 docker('pause',project+'-redis-1')
 try:report['redis_paused_status']=call(0);assert report['redis_paused_status']==503
 finally:docker('unpause',project+'-redis-1')
 time.sleep(2);report['recovered_status']=call(0);assert report['recovered_status']==200
 time.sleep(2);report['usage']=json.loads(req('/admin/usage?key='+k['limits']['id'])[1]);assert report['usage']['requests']>=33
 pathlib.Path(__file__).resolve().parents[1].joinpath('docs/results/storage-outages.json').write_text(json.dumps(report,indent=2));print(json.dumps(report,indent=2))
finally:req('/admin/keys/'+k['limits']['id'],method='DELETE')
