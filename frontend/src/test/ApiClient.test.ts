import {afterEach,describe,expect,it,vi} from 'vitest';
import {loadDashboard} from '../api/client';

afterEach(()=>vi.unstubAllGlobals());

describe('API credentials',()=>{
 it('transmits the API key only in X-API-Key headers',async()=>{
  const secret='header-only-secret-value';
  const log=vi.spyOn(console,'log');
  const fetchMock=vi.fn(async(input:RequestInfo|URL,_init?:RequestInit)=>{
   const url=String(input);let body:unknown=[];
   if(url.includes('/forecast/cash?'))body={generated_at:'x',from_ts:1,to_ts:2,horizon_days:7,method:'m',methodology:'m',source:{},currencies:{INR:{historical:[],forecast:[],totals:{historical_inflow:0,historical_outflow:0,historical_net:0,forecast_net:0},forecast_available:false,unavailable_reason:'none'}}};
   else if(url.endsWith('/metrics'))body={records_reconciled:0,candidates_created:0,open_exceptions_by_severity:{}};
   return {ok:true,status:200,json:async()=>body} as Response;
  });
  vi.stubGlobal('fetch',fetchMock);
  await loadDashboard(secret,1,2,7,'INR');
  expect(fetchMock).toHaveBeenCalled();
  for(const [url,init] of fetchMock.mock.calls){
   expect(String(url)).not.toContain(secret);
   expect(init?.headers).toEqual({'X-API-Key':secret});
  }
  expect(log).not.toHaveBeenCalled();
  log.mockRestore();
 });
});
