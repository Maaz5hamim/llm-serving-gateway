import http from 'k6/http';
import {check} from 'k6';
import {Trend} from 'k6/metrics';
const overhead = new Trend('gateway_overhead_ms');
const prompts = JSON.parse(open('../datasets/support.json'));
export const options = {summaryTrendStats:['avg','min','med','max','p(90)','p(95)','p(99)'],scenarios:{load:{executor:'constant-arrival-rate',rate:Number(__ENV.RATE || 50),timeUnit:'1s',duration:__ENV.DURATION || '30s',preAllocatedVUs:100,maxVUs:1000}},thresholds:{http_req_failed:['rate<0.01'],gateway_overhead_ms:['p(99)<10']}};
export default function(){
 const mode=__ENV.MODE || 'cache';
 const prompt=mode==='overhead'?`${__VU}-${__ITER}-${Date.now()}`:prompts[(__VU+__ITER)%prompts.length];
 const r=http.post(`${__ENV.BASE_URL || 'http://host.docker.internal:8080'}/v1/chat/completions`,JSON.stringify({model:'fast-chat',messages:[{role:'user',content:prompt}],temperature:0,max_tokens:64}),{headers:{Authorization:`Bearer ${__ENV.API_KEY}`,'Content-Type':'application/json','X-Cache':mode==='overhead'?'bypass':'force'}});
 check(r,{'completion succeeds':r=>r.status===200});
 if(r.headers['X-Gateway-Overhead-Ms'])overhead.add(Number(r.headers['X-Gateway-Overhead-Ms']));
}
