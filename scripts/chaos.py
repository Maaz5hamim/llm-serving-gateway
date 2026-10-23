#!/usr/bin/env python3
"""Local Compose chaos driver. Start k6 workload separately to measure client impact."""
import argparse,json,time,urllib.request
p=argparse.ArgumentParser();p.add_argument('scenario',choices=['latency','outage']);p.add_argument('--seconds',type=int,default=60);p.add_argument('--url',default='http://localhost:8474');a=p.parse_args()
def send(path,value,method='POST'):
 req=urllib.request.Request(a.url+path,data=json.dumps(value).encode(),headers={'Content-Type':'application/json'},method=method)
 with urllib.request.urlopen(req,timeout=5) as r:return r.read()
try:
 if a.scenario=='outage':send('/proxies/primary',{'enabled':False})
 else:send('/proxies/primary/toxics',{'name':'slow','type':'latency','stream':'downstream','attributes':{'latency':5000,'jitter':0}})
 print(f'{a.scenario} active for {a.seconds}s',flush=True);time.sleep(a.seconds)
finally:
 if a.scenario=='outage':send('/proxies/primary',{'enabled':True})
 else:send('/proxies/primary/toxics/slow',{},'DELETE')
 print('Primary restored. Allow 30s for breaker recovery probe.')
