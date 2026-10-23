#!/usr/bin/env python3
"""Run against a local Compose stack. Creates and revokes isolated API keys."""
import concurrent.futures,json,os,time,urllib.request,urllib.error,uuid
base=os.getenv('BASE_URL','http://localhost:8080');admin=os.environ['ADMIN_API_KEY']
def req(path,body=None,key=admin,method=None):
 r=urllib.request.Request(base+path,data=None if body is None else json.dumps(body).encode(),headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'},method=method)
 try:
  with urllib.request.urlopen(r,timeout=15) as res:return res.status,dict(res.headers),res.read()
 except urllib.error.HTTPError as e:return e.code,dict(e.headers),e.read()
def create(**limits):
 code,_,b=req('/admin/keys',{'name':'smoke-'+str(uuid.uuid4()),'rpm_limit':10000,'daily_tokens':10000000,**limits});assert code==201,(code,b);return json.loads(b)
created=[]
try:
 k=create();created.append(k);key=k['key'];prompt=str(uuid.uuid4());payload={'model':'fast-chat','messages':[{'role':'user','content':prompt}],'temperature':0,'max_tokens':32}
 code,h,b=req('/v1/chat/completions',payload,key);assert code==200,(code,b);assert h.get('X-Cache')=='miss',h
 code,h,c=req('/v1/chat/completions',payload,key);assert code==200 and b==c and h.get('X-Cache')=='hit',(code,h,c)
 stream={**payload,'stream':True};code,h,b=req('/v1/chat/completions',stream,key);assert code==200 and b.endswith(b'data: [DONE]\n\n'),(code,b)
 code,h,c=req('/v1/chat/completions',stream,key);assert b==c and h.get('X-Cache')=='hit',(code,h)
 payload['messages'][0]['content']=str(uuid.uuid4())
 with concurrent.futures.ThreadPoolExecutor(max_workers=100) as pool:
  results=list(pool.map(lambda _:req('/v1/chat/completions',payload,key),range(100)))
 assert all(x[0]==200 for x in results),[x[0] for x in results]
 assert sum(x[1].get('X-Cache')=='miss' for x in results)==1
 limited=create(rpm_limit=1);created.append(limited);assert req('/v1/chat/completions',payload,limited['key'])[0]==200;assert req('/v1/chat/completions',payload,limited['key'])[0]==429
 small=create(daily_tokens=1);created.append(small);assert req('/v1/chat/completions',payload,small['key'])[0]==402
 time.sleep(2);code,_,b=req('/admin/usage?key='+k['limits']['id']);v=json.loads(b);assert code==200 and v['requests']>=104 and v['saved_usd']>0,(code,v)
 print(json.dumps({'checks':'cache, SSE replay, 100-call coalescing, rate limit, budget, durable usage','usage':v},indent=2))
 assert req('/admin/keys/'+k['limits']['id'],key=admin,method='DELETE')[0]==204;assert req('/v1/chat/completions',payload,key)[0]==401
 print('Revocation: passed')
finally:
 for k in created:req('/admin/keys/'+k['limits']['id'],key=admin,method='DELETE')
