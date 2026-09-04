import type {ReactNode} from 'react';export function StatusPanel({title,children}:{title:string;children:ReactNode}){return <section className="panel"><h2>{title}</h2>{children}</section>}
