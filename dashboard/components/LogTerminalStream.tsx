"use client";

import React, { useState, useEffect, useRef } from 'react';
import { Terminal as TerminalIcon } from 'lucide-react';

interface LogEntry {
  timestamp: string;
  level: string;
  message: string;
}

interface LogTerminalStreamProps {
  logs: LogEntry[];
}

export const LogTerminalStream: React.FC<LogTerminalStreamProps> = ({ logs }) => {
  const [filter, setFilter] = useState<string>('ALL');
  const terminalEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    terminalEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

  const filteredLogs = logs.filter((log) => {
    if (filter === 'ALL') return true;
    return log.level.toUpperCase() === filter;
  });

  const getLevelBadgeClass = (level: string) => {
    switch (level.toUpperCase()) {
      case 'INFO':
        return 'text-primary bg-primary/10 border-primary/30';
      case 'ACK':
      case 'SUCCESS':
        return 'text-secondary bg-secondary/10 border-secondary/30';
      case 'WARN':
      case 'RETRY':
        return 'text-accent-amber bg-accent-amber/10 border-accent-amber/30';
      case 'ERR':
      case 'ERROR':
        return 'text-accent-crimson bg-accent-crimson/10 border-accent-crimson/30';
      default:
        return 'text-outline bg-surface-high border-outline-variant';
    }
  };

  const filterLabels = [
    { id: 'ALL', label: 'All' },
    { id: 'INFO', label: 'Info' },
    { id: 'ACK', label: 'Ack' },
    { id: 'WARN', label: 'Warn' },
    { id: 'SUCCESS', label: 'Success' },
  ];

  return (
    <div className="bg-surface-lowest border border-outline-variant rounded flex flex-col h-full text-xs">
      <div className="bg-surface-container px-4 py-2.5 border-b border-outline-variant flex justify-between items-center rounded-t">
        <div className="flex items-center gap-2">
          <TerminalIcon className="w-3.5 h-3.5 text-outline" />
          <span className="text-sm font-semibold text-foreground">Log stream</span>
        </div>
        <div className="flex items-center gap-1">
          {filterLabels.map(({ id, label }) => (
            <button
              key={id}
              onClick={() => setFilter(id)}
              className={`px-2.5 py-1 text-[11px] rounded-sm border transition-colors duration-150 active:scale-[0.97] ${
                filter === id
                  ? 'bg-primary/15 border-primary/50 text-primary font-medium'
                  : 'bg-transparent border-transparent text-outline hover:text-foreground'
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      <div className="p-3 overflow-y-auto max-h-[220px] min-h-[160px] custom-scrollbar space-y-1 font-mono">
        {filteredLogs.length === 0 ? (
          <div className="text-outline italic text-center py-4">
            No log entries match &ldquo;{filter}&rdquo;
          </div>
        ) : (
          filteredLogs.map((log, idx) => (
            <div key={idx} className="flex items-start gap-2 leading-relaxed">
              <span className="text-outline text-[10px] select-none shrink-0">{log.timestamp}</span>
              <span className={`px-1.5 py-0.5 border text-[10px] rounded-sm shrink-0 ${getLevelBadgeClass(log.level)}`}>
                {log.level.toLowerCase()}
              </span>
              <span className="text-foreground/90 break-all">{log.message}</span>
            </div>
          ))
        )}
        <div ref={terminalEndRef} />
      </div>
    </div>
  );
};
