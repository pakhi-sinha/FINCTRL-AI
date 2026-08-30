import type {DashboardData,ExceptionItem,Forecast,Investigation,Metrics,Run,SyncState} from '../types/dashboard';
const base=(import.meta.env.VITE_API_BASE_URL||'http://localhost:8000').replace(/\/$/,'');
export class ApiError extends Error{constructor(public status:number,message:string){super(message)}}
async function get<T>(path:string,key:string):Promise<T>{const response=await fetch(`${base}${path}`,{headers:{'X-API-Key':key}});if(!response.ok){let message=`Request failed (${response.status})`;try{message=(await response.json()).detail||message}catch{}throw new ApiError(response.status,message)}return response.json() as Promise<T>}
export async function loadDashboard(key:string,fromTs:number,toTs:number,horizon:number,currency:string):Promise<DashboardData>{
 const query=`from_ts=${fromTs}&to_ts=${toTs}&horizon_days=${horizon}&currency=${currency}`;
 const [forecast,metrics,exceptions,runs,sync]=await Promise.all([get<Forecast>(`/forecast/cash?${query}`,key),get<Metrics>('/metrics',key),get<ExceptionItem[]>('/exceptions',key),get<Run[]>('/reconciliation/runs',key),get<SyncState[]>('/razorpay/sync-status',key)]);
 const investigations=(await Promise.all(exceptions.map(item=>get<Investigation[]>(`/reconciliation/exceptions/${item.id}/investigations`,key)))).flat();
 return {forecast,metrics,exceptions,investigations,runs,sync};
}
