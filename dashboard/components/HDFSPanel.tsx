"use client";

import React, { useState } from 'react';
import { HardDrive, UploadCloud, CheckCircle2, AlertCircle } from 'lucide-react';

interface HDFSPanelProps {
  hdfsData: {
    status: string;
    type: string;
    endpoint: string;
    hdfs_path: string;
    pushed_files_count: number;
    total_bytes_pushed: number;
    last_sync_timestamp: string | null;
  };
  onSync: () => Promise<void>;
}

// Returns a human-readable "X min ago" label for the last sync time.
// Skill: users must know if data is stale.
function getLastSyncAge(timestamp: string | null): string {
  if (!timestamp) return 'Never synced';
  const then = new Date(timestamp).getTime();
  if (isNaN(then)) return timestamp;
  const diffMs = Date.now() - then;
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 1) return 'Just now';
  if (diffMin < 60) return `${diffMin} min ago`;
  const diffHr = Math.floor(diffMin / 60);
  return `${diffHr}h ${diffMin % 60}m ago`;
}

export const HDFSPanel: React.FC<HDFSPanelProps> = ({ hdfsData, onSync }) => {
  const [isSyncing, setIsSyncing] = useState(false);
  const [syncMsg, setSyncMsg] = useState<string | null>(null);
  const [syncOk, setSyncOk] = useState(true);

  const handleSyncClick = async () => {
    setIsSyncing(true);
    setSyncMsg(null);
    try {
      await onSync();
      setSyncMsg('HDFS push completed successfully');
      setSyncOk(true);
    } catch (err: any) {
      setSyncMsg('HDFS push trigger error');
      setSyncOk(false);
    } finally {
      setIsSyncing(false);
    }
  };

  const formatBytes = (bytes: number) => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  // Skill: status color encodes health — green = on track, red = alert
  const isConnected = !hdfsData.status || hdfsData.status.toUpperCase().includes('CONNECT');
  const connColor = isConnected ? 'text-secondary' : 'text-accent-crimson';
  const connDot = isConnected ? 'bg-secondary status-pulse' : 'bg-accent-crimson';

  const syncAge = getLastSyncAge(hdfsData.last_sync_timestamp);
  const syncIsStale = !hdfsData.last_sync_timestamp ||
    (Date.now() - new Date(hdfsData.last_sync_timestamp).getTime()) > 60 * 60 * 1000; // > 1h

  return (
    <div id="hdfs" className="bg-surface-container border border-outline-variant rounded p-5">
      {/* Panel header */}
      <div className="flex flex-wrap justify-between items-center mb-5 pb-3 border-b border-outline-variant gap-2">
        <div className="flex items-center gap-2">
          <HardDrive className="w-4 h-4 text-outline" />
          <h2 className="text-sm font-semibold text-foreground">HDFS sync</h2>
        </div>
        <div className="flex items-center gap-2 text-xs">
          <span className="text-outline">Target path</span>
          <code className="bg-surface-lowest border border-outline-variant px-2 py-0.5 text-primary text-[11px] rounded-sm font-mono">
            {hdfsData.hdfs_path || '/data/raw/'}
          </code>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
        {/* Connection state — skill: status color must encode health */}
        <div className="bg-surface-lowest border border-outline-variant p-4 flex flex-col gap-3 rounded">
          <div className="text-[11px] text-outline">Connection state</div>
          <div className="flex items-center gap-2">
            <span className={`w-2 h-2 rounded-full shrink-0 ${connDot}`} />
            <span className={`text-sm font-semibold font-mono ${connColor}`}>
              {hdfsData.status || 'Connected'}
            </span>
          </div>
          <div className="text-[11px] text-outline truncate" title={hdfsData.endpoint}>
            {hdfsData.type} · {hdfsData.endpoint}
          </div>
        </div>

        {/* Files committed */}
        <div className="bg-surface-lowest border border-outline-variant p-4 flex flex-col gap-3 rounded">
          <div className="text-[11px] text-outline">Files committed</div>
          <div className="text-3xl font-bold text-foreground font-mono leading-none">
            {hdfsData.pushed_files_count}
          </div>
          {/* Skill: show supporting context below the headline number */}
          <div className="text-[11px] text-outline">{formatBytes(hdfsData.total_bytes_pushed)} pushed total</div>
        </div>

        {/* Last sync + action — skill: "last updated" timestamp tells users if data is stale */}
        <div className="bg-surface-lowest border border-outline-variant p-4 flex flex-col gap-3 rounded">
          <div className="text-[11px] text-outline">Last sync</div>
          <div className="flex flex-col gap-0.5">
            <span className={`text-sm font-semibold font-mono ${syncIsStale ? 'text-accent-amber' : 'text-foreground'}`}>
              {syncAge}
            </span>
            {hdfsData.last_sync_timestamp && (
              <span className="text-[10px] text-outline font-mono">
                {hdfsData.last_sync_timestamp}
              </span>
            )}
          </div>
          <button
            onClick={handleSyncClick}
            disabled={isSyncing}
            className="w-full bg-tertiary/10 border border-tertiary/50 text-tertiary font-semibold text-sm py-2 px-3 flex items-center justify-center gap-2 rounded transition-all duration-150 hover:bg-tertiary/20 disabled:opacity-50 active:scale-[0.97]"
          >
            <UploadCloud className="w-4 h-4" />
            <span>{isSyncing ? 'Syncing…' : 'Sync to HDFS'}</span>
          </button>
        </div>
      </div>

      {/* Feedback banner — skill: errors explain what happened and what to do */}
      {syncMsg && (
        <div className={`p-2.5 border text-xs mb-3 flex items-center gap-2 rounded ${
          syncOk
            ? 'bg-secondary/10 border-secondary/40 text-secondary'
            : 'bg-accent-crimson/10 border-accent-crimson/40 text-accent-crimson'
        }`}>
          {syncOk
            ? <CheckCircle2 className="w-4 h-4 shrink-0" />
            : <AlertCircle className="w-4 h-4 shrink-0" />
          }
          <span>{syncMsg}</span>
        </div>
      )}
    </div>
  );
};
