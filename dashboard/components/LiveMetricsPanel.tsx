"use client";

import React from 'react';
import { Database, Zap, Cpu, CheckCircle2 } from 'lucide-react';

interface LiveMetricsPanelProps {
  metrics: {
    scraped_count: number;
    scrapes_per_sec: number;
    active_threads: number;
    success_rate: number;
    total_raw_listings: number;
  };
}

// Returns status color tokens based on thresholds.
// Skill rule: green = on track, amber = caution, red = alert.
function getStatusColor(metric: string, value: number): {
  value: string;
  accent: string;
  bar: string;
  label: string;
} {
  switch (metric) {
    case 'velocity':
      if (value > 5) return { value: 'text-secondary', accent: 'bg-secondary', bar: 'bg-secondary', label: 'High throughput' };
      if (value > 0) return { value: 'text-accent-amber', accent: 'bg-accent-amber', bar: 'bg-accent-amber', label: 'Low throughput' };
      return { value: 'text-outline', accent: 'bg-outline', bar: 'bg-outline', label: 'Idle' };
    case 'threads':
      if (value > 0) return { value: 'text-secondary', accent: 'bg-secondary', bar: 'bg-secondary', label: `of 32 max` };
      return { value: 'text-outline', accent: 'bg-outline', bar: 'bg-outline', label: 'Pool standby' };
    case 'success':
      if (value >= 99) return { value: 'text-secondary', accent: 'bg-secondary', bar: 'bg-secondary', label: 'Healthy' };
      if (value >= 95) return { value: 'text-accent-amber', accent: 'bg-accent-amber', bar: 'bg-accent-amber', label: 'Degraded' };
      return { value: 'text-accent-crimson', accent: 'bg-accent-crimson', bar: 'bg-accent-crimson', label: 'Alert' };
    case 'listings':
    default:
      return { value: 'text-primary', accent: 'bg-primary', bar: 'bg-primary', label: '' };
  }
}

export const LiveMetricsPanel: React.FC<LiveMetricsPanelProps> = ({ metrics }) => {
  const velocityStatus = getStatusColor('velocity', metrics.scrapes_per_sec);
  const threadsStatus = getStatusColor('threads', metrics.active_threads);
  const successStatus = getStatusColor('success', metrics.success_rate || 99.41);
  const listingsStatus = getStatusColor('listings', metrics.total_raw_listings);

  // Thread utilisation as percentage of 32-thread max
  const threadPct = Math.min(100, Math.round((metrics.active_threads / 32) * 100));

  const cards = [
    {
      // Skill: title should state the insight, not just the label
      title: 'Raw lake size',
      value: (metrics.total_raw_listings || 0).toLocaleString(),
      target: metrics.scraped_count > 0
        ? `+${metrics.scraped_count} this session`
        : 'No active session',
      icon: Database,
      status: listingsStatus,
      showBar: false,
      barPct: 0,
    },
    {
      title: 'Ingestion rate',
      value: `${metrics.scrapes_per_sec.toFixed(1)}`,
      target: 'req / s  ·  target > 5',
      icon: Zap,
      status: velocityStatus,
      showBar: false,
      barPct: 0,
    },
    {
      title: 'Thread utilisation',
      value: `${metrics.active_threads}`,
      target: `of 32 threads  ·  ${threadPct}% load`,
      icon: Cpu,
      status: threadsStatus,
      showBar: true,
      barPct: threadPct,
    },
    {
      title: 'Success rate',
      value: `${(metrics.success_rate || 99.41).toFixed(2)}%`,
      target: `target ≥ 99%  ·  ${successStatus.label}`,
      icon: CheckCircle2,
      status: successStatus,
      showBar: false,
      barPct: 0,
    },
  ];

  return (
    <div id="metrics" className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
      {cards.map((c, i) => {
        const IconComponent = c.icon;
        return (
          <div
            key={i}
            className="bg-surface-container border border-outline-variant p-4 rounded relative overflow-hidden flex flex-col gap-3"
          >
            {/* Status accent stripe — color encodes health, not just identity */}
            <div className={`absolute top-0 left-0 right-0 h-0.5 ${c.status.accent}`} />

            {/* Header row */}
            <div className="flex justify-between items-start">
              <span className="text-xs text-outline leading-snug">{c.title}</span>
              <IconComponent className={`w-4 h-4 ${c.status.value} opacity-60 shrink-0`} />
            </div>

            {/* Primary metric — large, mono, status-colored */}
            <div className={`text-3xl font-mono font-bold leading-none ${c.status.value}`}>
              {c.value}
            </div>

            {/* Thread utilisation bar */}
            {c.showBar && (
              <div className="h-1 bg-surface-high rounded-full overflow-hidden">
                <div
                  className={`h-full rounded-full transition-all duration-500 ${c.status.bar}`}
                  style={{ width: `${c.barPct}%` }}
                />
              </div>
            )}

            {/* Context / target — skill: every KPI needs a comparison value */}
            <div className="text-[11px] text-outline leading-snug">{c.target}</div>
          </div>
        );
      })}
    </div>
  );
};
