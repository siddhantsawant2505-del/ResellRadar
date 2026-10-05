"use client";

import React from 'react';
import { GitBranch, Network, Cpu, MemoryStick, CircleCheck, CircleSlash, Loader2 } from 'lucide-react';

interface ClusterWorker {
  name: string;
  status: string;
  cpu?: string;
  mem_used?: string;
  mem_limit?: string;
}

interface RunningApp {
  name: string;
  state: string;
  status?: string;
}

interface FinishedApp {
  name: string;
  container: string;
  state: string;
  finished_at: string;
}

interface ClusterPanelProps {
  cluster: {
    standalone_up: boolean;
    master_url: string;
    mode: string;
    worker_count: number;
    workers: ClusterWorker[];
    running_apps: RunningApp[];
    finished_apps: FinishedApp[];
  } | null;
}

// Branches hanging off the master node — one card per Spark Standalone worker unit.
export const ClusterPanel: React.FC<ClusterPanelProps> = ({ cluster }) => {
  if (!cluster) return null;

  const up = cluster.standalone_up;
  const running = cluster.running_apps || [];
  const finished = cluster.finished_apps || [];

  const parseCpu = (cpu?: string) => Math.min(100, parseFloat((cpu || '0%').replace('%', '')) || 0);

  return (
    <div id="cluster" className="bg-surface-container border border-outline-variant rounded p-5">
      {/* Panel header */}
      <div className="flex flex-wrap justify-between items-center mb-5 pb-3 border-b border-outline-variant gap-2">
        <div className="flex items-center gap-2">
          <GitBranch className="w-4 h-4 text-outline" />
          <h2 className="text-sm font-semibold text-foreground">
            Batch cluster
            <span className="ml-2 text-xs font-normal text-outline font-mono">
              {up ? `${cluster.worker_count} worker units` : 'not running'}
            </span>
          </h2>
        </div>
        <div className="flex items-center gap-2 text-xs">
          <span
            className={`w-2 h-2 rounded-full ${up ? 'bg-secondary status-pulse' : 'bg-outline'}`}
          />
          <span className={`font-mono text-[11px] ${up ? 'text-secondary' : 'text-outline'}`}>
            {up ? 'DISTRIBUTED' : 'SINGLE-UNIT FALLBACK'}
          </span>
        </div>
      </div>

      {/* Execution flow: driver -> master -> branched workers */}
      <div className="mb-5 rounded border border-outline-variant bg-surface-lowest p-4 overflow-x-auto">
        <div className="flex items-center gap-3 min-w-[720px]">
          {/* Driver */}
          <div className="flex flex-col items-center gap-1 shrink-0">
            <div className={`px-3 py-2 rounded border text-[11px] font-mono text-center ${
              up ? 'border-primary/50 bg-primary/10 text-primary' : 'border-outline-variant bg-surface-container text-outline'
            }`}>
              spark-submit<br />driver
            </div>
          </div>

          <div className={`w-10 h-px shrink-0 ${up ? 'bg-primary/50' : 'bg-outline-variant'}`} />

          {/* Master */}
          <div className="flex flex-col items-center gap-1 shrink-0">
            <div className={`px-3 py-2 rounded border text-[11px] font-mono text-center ${
              up ? 'border-secondary/50 bg-secondary/10 text-secondary' : 'border-outline-variant bg-surface-container text-outline'
            }`}>
              Spark Master<br />spark://spark-master:7077
            </div>
          </div>

          {/* Branch lines to each worker */}
          <div className="flex items-center shrink-0">
            {up && cluster.workers.map((_, i) => (
              <div key={i} className="w-6 h-px bg-secondary/40" />
            ))}
            {!up && <div className="w-6 h-px bg-outline-variant" />}
          </div>

          {/* Worker branch cards */}
          <div className="flex gap-3 flex-1">
            {up && cluster.workers.map((w) => {
              const cpu = parseCpu(w.cpu);
              const busy = cpu > 5;
              return (
                <div
                  key={w.name}
                  className={`flex-1 min-w-[190px] rounded border p-3 ${
                    busy ? 'border-secondary/60 bg-secondary/5' : 'border-outline-variant bg-surface-container'
                  }`}
                >
                  <div className="flex items-center justify-between gap-2 mb-2">
                    <span className="text-[11px] font-mono text-foreground truncate" title={w.name}>
                      {w.name.replace('resellradar-spark-', '')}
                    </span>
                    {busy ? (
                      <Loader2 className="w-3.5 h-3.5 text-secondary animate-spin" />
                    ) : (
                      <CircleCheck className="w-3.5 h-3.5 text-outline" />
                    )}
                  </div>
                  <div className="flex items-center gap-1.5 text-[10px] text-outline font-mono mb-1.5">
                    <Cpu className="w-3 h-3" />
                    <span className={busy ? 'text-secondary font-semibold' : ''}>{w.cpu || '—'}</span>
                    <span>cpu</span>
                  </div>
                  <div className="h-1 bg-surface-high rounded-full overflow-hidden mb-2">
                    <div
                      className={`h-full rounded-full transition-all duration-500 ${busy ? 'bg-secondary' : 'bg-outline'}`}
                      style={{ width: `${Math.max(2, cpu)}%` }}
                    />
                  </div>
                  <div className="flex items-center gap-1.5 text-[10px] text-outline font-mono">
                    <MemoryStick className="w-3 h-3" />
                    <span>{w.mem_used || '—'}</span>
                    <span className="opacity-60">/ {w.mem_limit || '—'}</span>
                  </div>
                </div>
              );
            })}
            {!up && (
              <div className="flex-1 min-w-[190px] rounded border border-dashed border-outline-variant p-3 text-[11px] text-outline">
                Worker units offline — pipeline runs on one machine (
                <code className="font-mono">local[*]</code>). Start the cluster:
                <code className="block mt-1 font-mono text-[10px] text-primary">
                  docker compose --profile cluster up -d --scale spark-worker=3
                </code>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Applications table */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div>
          <div className="flex items-center gap-1.5 text-xs text-outline mb-2">
            <Network className="w-3.5 h-3.5" />
            Running / queued applications
          </div>
          {running.length === 0 ? (
            <div className="text-[11px] text-outline italic border border-dashed border-outline-variant rounded p-3">
              No active Spark application — trigger the pipeline to see the worker units branch out.
            </div>
          ) : (
            running.map((a) => (
              <div key={a.name} className="flex items-center gap-2 text-[11px] font-mono text-foreground border border-secondary/40 bg-secondary/5 rounded p-2 mb-1.5">
                <Loader2 className="w-3.5 h-3.5 text-secondary animate-spin shrink-0" />
                <span className="truncate">{a.name.replace('resellradar-spark-console-', 'driver: ')}</span>
                <span className="ml-auto text-outline shrink-0">{a.state}</span>
              </div>
            ))
          )}
        </div>
        <div>
          <div className="flex items-center gap-1.5 text-xs text-outline mb-2">
            <CircleCheck className="w-3.5 h-3.5" />
            Completed applications
          </div>
          {finished.length === 0 ? (
            <div className="text-[11px] text-outline italic border border-dashed border-outline-variant rounded p-3">
              Nothing completed yet.
            </div>
          ) : (
            <div className="max-h-[150px] overflow-y-auto custom-scrollbar">
              {finished.slice(0, 10).map((a) => (
                <div key={a.container} className="flex items-center gap-2 text-[11px] font-mono text-outline border border-outline-variant/60 rounded p-2 mb-1.5">
                  <CircleCheck className={`w-3.5 h-3.5 shrink-0 ${a.state === 'FINISHED' ? 'text-secondary' : 'text-accent-crimson'}`} />
                  <span className="text-foreground/80 truncate">{a.name}</span>
                  <span className="ml-auto shrink-0">{a.state === 'FINISHED' ? 'done' : a.state.toLowerCase()}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
