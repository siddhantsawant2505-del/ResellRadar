"use client";

import React, { useState, useEffect } from 'react';
import Link from 'next/link';
import { RefreshCw, Activity, HardDrive, Cpu, BarChart3 } from 'lucide-react';

interface HeaderNavProps {
  state: any;
  onRefresh: () => void;
}

export const HeaderNav: React.FC<HeaderNavProps> = ({ state, onRefresh }) => {
  const isRunning = state?.pipeline_state?.is_running;
  const hdfsStatus = state?.hdfs_telemetry?.status || 'Connected';

  // Skill: "Data source and refresh date must be visible in a footer/header"
  const [lastRefreshed, setLastRefreshed] = useState<string>('—');
  useEffect(() => {
    const now = new Date();
    setLastRefreshed(now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }));
  }, [state]);

  return (
    <header className="flex flex-wrap justify-between items-center w-full px-6 py-3 bg-surface-lowest border-b border-outline-variant sticky top-0 z-50">
      <div className="flex items-center gap-5">
        <div className="flex items-center gap-2.5">
          <span
            className={`w-2.5 h-2.5 rounded-full ${
              isRunning ? 'bg-secondary status-pulse' : 'bg-outline'
            }`}
          />
          <h1 className="text-base font-semibold tracking-tight text-foreground">
            ResellRadar
            <span className="text-xs text-outline font-normal font-mono ml-2">ingestion console</span>
          </h1>
        </div>

        <nav className="hidden md:flex items-center gap-1">
          {[
            { href: '#metrics', label: 'Metrics' },
            { href: '#controller', label: 'Controller' },
            { href: '#cluster', label: 'Cluster' },
            { href: '#hdfs', label: 'HDFS sync' },
            { href: '#stream', label: 'Raw stream' },
          ].map((item) => (
            <a
              key={item.href}
              href={item.href}
              className="px-3 py-1.5 text-sm text-outline hover:text-foreground rounded transition-colors duration-150"
            >
              {item.label}
            </a>
          ))}
          <Link
            href="/analytics"
            className="px-3 py-1.5 text-sm text-primary hover:text-primary-dim rounded transition-colors duration-150 flex items-center gap-1.5"
          >
            <BarChart3 className="w-3.5 h-3.5" />
            Analytics
          </Link>
        </nav>
      </div>

      {/* Cluster diagnostics pill */}
      <div className="hidden lg:flex items-center gap-3 px-3 py-1.5 rounded bg-surface-container border border-outline-variant text-xs">
        <div className="flex items-center gap-1.5">
          <Cpu className="w-3.5 h-3.5 text-outline" />
          <span className="text-outline">Node</span>
          <span className="text-foreground font-mono text-[11px]">localhost-master-01</span>
        </div>
        <div className="h-3 w-px bg-outline-variant" />
        <div className="flex items-center gap-1.5">
          <Activity className="w-3.5 h-3.5 text-outline" />
          <span className="text-outline">State</span>
          <span className={`font-mono text-[11px] font-semibold ${
            isRunning ? 'text-secondary status-pulse' : 'text-foreground'
          }`}>
            {isRunning ? 'Streaming' : 'Idle'}
          </span>
        </div>
        <div className="h-3 w-px bg-outline-variant" />
        <div className="flex items-center gap-1.5">
          <HardDrive className="w-3.5 h-3.5 text-outline" />
          <span className="text-outline">HDFS</span>
          <span className="text-secondary font-mono text-[11px]">{hdfsStatus}</span>
        </div>
      </div>

      <div className="flex items-center gap-3">
        {/* Skill: "last updated timestamp visible so users know if data is stale" */}
        <span className="hidden sm:block text-[11px] text-outline font-mono">
          Updated {lastRefreshed}
        </span>
        <button
          onClick={onRefresh}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-outline border border-outline-variant rounded hover:text-foreground hover:border-outline transition-colors duration-150 active:scale-[0.97]"
          title="Refresh telemetry"
        >
          <RefreshCw className="w-3.5 h-3.5" />
          <span>Refresh</span>
        </button>
      </div>
    </header>
  );
};
