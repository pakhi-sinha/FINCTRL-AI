import {useCallback,useEffect,useState} from 'react';
import {ApiError,loadDashboard} from '../api/client';
import type {DashboardData} from '../types/dashboard';
export function useDashboard(key:string,fromTs:number,toTs:number,horizon:number,currency:string){
 const [data,setData]=useState<DashboardData|null>(null);const[loading,setLoading]=useState(true);const[error,setError]=useState<string|null>(null);const[authStatus,setAuthStatus]=useState<number|null>(null);
 const refresh=useCallback(async()=>{if(!key){setLoading(false);setError('Enter an API key to load operational data.');setAuthStatus(401);return}setLoading(true);setError(null);setAuthStatus(null);try{setData(await loadDashboard(key,fromTs,toTs,horizon,currency))}catch(e){setData(null);setError(e instanceof Error?e.message:'Unexpected API response');setAuthStatus(e instanceof ApiError?e.status:null)}finally{setLoading(false)}},[key,fromTs,toTs,horizon,currency]);
 useEffect(()=>{void refresh()},[refresh]);return{data,loading,error,authStatus,refresh};
}
