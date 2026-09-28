"use client";

import React, { useState, useEffect, useCallback } from 'react';
import { HeaderNav } from '../components/HeaderNav';
import { JobTriggerPanel } from '../components/JobTriggerPanel';
import { LiveMetricsPanel } from '../components/LiveMetricsPanel';
import { LogTerminalStream } from '../components/LogTerminalStream';
import { RawDataGrid } from '../components/RawDataGrid';
import { HDFSPanel } from '../components/HDFSPanel';

export default function Home() {
  const [serverState, setServerState] = useState<any>(null);
  const [logs, setLogs] = useState<any[]>([]);
  const [gridData, setGridData] = useState<{ items: any[]; total: number; page: number }>({
    items: [],
    total: 0,
    page: 1,
  });
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [categoryFilter, setCategoryFilter] = useState<string>('all');
  const [page, setPage] = useState<number>(1);

  // Fetch telemetry status
  const fetchStatus = useCallback(async () => {
    try {
      const res = await fetch('/api/status');
      if (res.ok) {
        const data = await res.json();
        setServerState(data);
      }
    } catch (err) {
      console.error('Failed to fetch status', err);
    }
  }, []);

  // Fetch logs
  const fetchLogs = useCallback(async () => {
    try {
      const res = await fetch('/api/logs');
      if (res.ok) {
        const data = await res.json();
        setLogs(data.logs || []);
      }
    } catch (err) {
      console.error('Failed to fetch logs', err);
    }
  }, []);

  // Fetch raw preview grid
  const fetchPreview = useCallback(async () => {
    try {
      const params = new URLSearchParams({
        page: page.toString(),
        limit: '10',
      });
      if (categoryFilter && categoryFilter !== 'all') {
        params.append('category', categoryFilter);
      }
      if (searchQuery) {
        params.append('search', searchQuery);
      }
      const res = await fetch(`/api/preview?${params.toString()}`);
      if (res.ok) {
        const data = await res.json();
        setGridData({
          items: data.items || [],
          total: data.total || 0,
          page: data.page || 1,
        });
      }
    } catch (err) {
      console.error('Failed to fetch preview', err);
    }
  }, [page, categoryFilter, searchQuery]);

  // Polling loop
  useEffect(() => {
    fetchStatus();
    fetchLogs();
    fetchPreview();

    const interval = setInterval(() => {
      fetchStatus();
      fetchLogs();
      fetchPreview();
    }, 1500);

    return () => clearInterval(interval);
  }, [fetchStatus, fetchLogs, fetchPreview]);

  // Trigger scrape handler
  const handleStartScrape = async (category: string, target: number, threads: number) => {
    try {
      await fetch('/api/scrape/trigger', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ category, batch_target: target, concurrency_threads: threads }),
      });
      fetchStatus();
      fetchLogs();
    } catch (err) {
      console.error('Start scrape failed', err);
    }
  };

  // Stop scrape handler
  const handleStopScrape = async () => {
    try {
      await fetch('/api/scrape/stop', { method: 'POST' });
      fetchStatus();
      fetchLogs();
    } catch (err) {
      console.error('Stop scrape failed', err);
    }
  };

  // Sync to HDFS handler
  const handleSyncHDFS = async () => {
    try {
      await fetch('/api/hdfs/sync', { method: 'POST' });
      fetchStatus();
      fetchLogs();
    } catch (err) {
      console.error('HDFS sync failed', err);
    }
  };

  const isRunning = serverState?.pipeline_state?.is_running || false;
  const metrics = {
    scraped_count: serverState?.pipeline_state?.scraped_count || 0,
    scrapes_per_sec: serverState?.pipeline_state?.scrapes_per_sec || 0,
    active_threads: serverState?.pipeline_state?.active_threads || 0,
    success_rate: serverState?.pipeline_state?.success_rate || 99.41,
    total_raw_listings: serverState?.raw_storage?.total_listings_scraped || 0,
  };

  return (
    <div className="min-h-screen flex flex-col bg-surface text-foreground">
      <HeaderNav state={serverState} onRefresh={fetchStatus} />

      <main className="flex-1 p-4 md:p-6 max-w-[1700px] w-full mx-auto space-y-6">
        {/* Level 1: Telemetry Headline KPIs (F-pattern scan) */}
        <LiveMetricsPanel metrics={metrics} />

        {/* Level 2: Controller & Live Log Terminal */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          <div className="lg:col-span-5">
            <JobTriggerPanel isRunning={isRunning} onStart={handleStartScrape} onStop={handleStopScrape} />
          </div>
          <div className="lg:col-span-7">
            <LogTerminalStream logs={logs} />
          </div>
        </div>

        {/* Level 3: HDFS Sync Control */}
        <HDFSPanel hdfsData={serverState?.hdfs_telemetry || {}} onSync={handleSyncHDFS} />

        {/* Level 3: Raw Ingestion Stream Grid */}
        <RawDataGrid
          items={gridData.items}
          total={gridData.total}
          page={gridData.page}
          onPageChange={(p) => setPage(p)}
          search={searchQuery}
          onSearchChange={(q) => {
            setSearchQuery(q);
            setPage(1);
          }}
          categoryFilter={categoryFilter}
          onCategoryFilterChange={(cat) => {
            setCategoryFilter(cat);
            setPage(1);
          }}
        />
      </main>

      <footer className="border-t border-outline-variant bg-surface-lowest px-6 py-3 text-center text-xs font-mono text-outline">
        ResellRadar Data Ingestion Pipeline &middot; Data Engineering Layer &middot; HDFS target: <code>/data/raw/</code>
      </footer>
    </div>
  );
}
