"use client";

import React, { useState, useEffect, useCallback } from 'react';
import Link from 'next/link';
import {
  TrendingUp, DollarSign, Package, Repeat, Clock, Tag, BarChart3,
  CheckCircle2, AlertTriangle, Info, LineChart, MapPin, CalendarDays, Loader2, Layers,
} from 'lucide-react';
import { SimpleBarChart, HorizontalBars, SimpleLineChart, SimpleDonut } from '../../components/SimpleCharts';

// ---------------------------------------------------------------------------
// Analytics dashboard: KPIs, charts and auto-generated insights computed from
// the processed parquet tables by the backend's GET /api/analytics.
// ---------------------------------------------------------------------------

interface ChartPoint {
  label: string;
  value: number | null;
  extra?: number;
}

interface Insight {
  icon: string;
  title: string;
  body: string;
}

interface AnalyticsData {
  generated_at: string;
  ttl_seconds: number;
  cached?: boolean;
  stale?: boolean;
  data_coverage?: {
    full_corpus_rows: number;
    synthetic_source_rows: number;
    synthetic_share_pct: number;
  };
  tables?: Record<string, number>;
  kpis: Record<string, number>;
  charts: Record<string, ChartPoint[]>;
  insights: Insight[];
}

interface ChartCardProps {
  title: string;
  icon: React.ReactNode;
  subtitle?: string;
  children: React.ReactNode;
  className?: string;
}

const ChartCard: React.FC<ChartCardProps> = ({ title, icon, subtitle, children, className = '' }) => (
  <div className={`bg-surface-container border border-outline-variant rounded p-4 ${className}`}>
    <div className="flex items-center gap-2 mb-3 pb-2 border-b border-outline-variant/60">
      <span className="text-outline">{icon}</span>
      <h3 className="text-xs font-semibold text-foreground">{title}</h3>
      {subtitle && <span className="ml-auto text-[10px] font-mono text-outline">{subtitle}</span>}
    </div>
    {children}
  </div>
);

const KpiCard: React.FC<{
  label: string;
  value: string;
  sub?: string;
  icon: React.ReactNode;
  accent?: boolean;
}> = ({ label, value, sub, icon, accent }) => (
  <div
    className={`rounded border p-4 flex flex-col gap-1 ${
      accent ? 'border-primary/40 bg-primary/5' : 'border-outline-variant bg-surface-container'
    }`}
  >
    <div className="flex items-center gap-2 text-outline">
      {icon}
      <span className="text-[10px] uppercase tracking-wider font-semibold">{label}</span>
    </div>
    <span className="text-2xl font-semibold font-mono text-foreground leading-tight">
      {value}
    </span>
    {sub && <span className="text-[10px] text-outline font-mono">{sub}</span>}
  </div>
);

const InsightIcon: React.FC<{ icon: string }> = ({ icon }) => {
  const cls = 'w-4 h-4 shrink-0 mt-0.5';
  if (icon === 'check') return <CheckCircle2 className={`${cls} text-secondary`} />;
  if (icon === 'alert') return <AlertTriangle className={`${cls} text-accent-amber`} />;
  if (icon === 'trend') return <TrendingUp className={`${cls} text-primary`} />;
  return <Info className={`${cls} text-tertiary`} />;
};

const fmtInt = (n?: number | null) => (typeof n === 'number' ? n.toLocaleString() : '—');
const fmtMoney = (n?: number | null) =>
  typeof n === 'number'
    ? `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
    : '—';
const money = (v: number) => `$${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;

export default function AnalyticsPage() {
  const [data, setData] = useState<AnalyticsData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const load = useCallback(async (soft: boolean) => {
    if (soft) setRefreshing(true);
    try {
      const res = await fetch('/api/analytics', { cache: 'no-store' });
      const json = await res.json();
      if (json.error) {
        setError(json.error);
      } else {
        setData(json);
        setError(null);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    load(false);
    const interval = setInterval(() => load(true), 60000);
    return () => clearInterval(interval);
  }, [load]);

  const kpis = data?.kpis ?? {};
  const charts = data?.charts ?? {};

  return (
    <div className="min-h-screen flex flex-col bg-surface text-foreground">
      {/* Header (mirrors the console header style) */}
      <header className="flex flex-wrap justify-between items-center w-full px-6 py-3 bg-surface-lowest border-b border-outline-variant sticky top-0 z-50">
        <div className="flex items-center gap-5">
          <div className="flex items-center gap-2.5">
            <span className="w-2.5 h-2.5 rounded-full bg-primary" />
            <h1 className="text-base font-semibold tracking-tight text-foreground">
              ResellRadar
              <span className="text-xs text-outline font-normal font-mono ml-2">analytics</span>
            </h1>
          </div>
          <nav className="hidden md:flex items-center gap-1">
            <Link
              href="/"
              className="px-3 py-1.5 text-sm text-outline hover:text-foreground rounded transition-colors duration-150"
            >
              &larr; Ingestion console
            </Link>
            <a
              href="#charts"
              className="px-3 py-1.5 text-sm text-outline hover:text-foreground rounded transition-colors duration-150"
            >
              Charts
            </a>
            <a
              href="#insights"
              className="px-3 py-1.5 text-sm text-outline hover:text-foreground rounded transition-colors duration-150"
            >
              Insights
            </a>
          </nav>
        </div>

        <div className="flex items-center gap-3">
          {data && (
            <span className="hidden sm:block text-[11px] text-outline font-mono">
              computed {new Date(data.generated_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
              {data.stale ? ' · refreshing…' : data.cached ? ' · cached' : ' · fresh'}
            </span>
          )}
          <button
            onClick={() => load(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-outline border border-outline-variant rounded hover:text-foreground hover:border-outline transition-colors duration-150 active:scale-[0.97] disabled:opacity-50"
            disabled={refreshing}
            title="Re-fetch aggregates (recomputes server-side when stale)"
          >
            {refreshing ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <BarChart3 className="w-3.5 h-3.5" />}
            <span>Refresh</span>
          </button>
        </div>
      </header>

      <main className="flex-1 p-4 md:p-6 max-w-[1700px] w-full mx-auto space-y-6">
        {loading && (
          <div className="flex items-center justify-center gap-3 py-24 text-outline">
            <Loader2 className="w-5 h-5 animate-spin" />
            <span className="text-sm font-mono">Aggregating 2M+ processed listings…</span>
          </div>
        )}

        {error && !data && (
          <div className="border border-accent-crimson/50 bg-accent-crimson/5 rounded p-6 text-sm text-foreground">
            <div className="flex items-center gap-2 font-semibold mb-1">
              <AlertTriangle className="w-4 h-4 text-accent-crimson" /> Analytics unavailable
            </div>
            <p className="text-outline text-xs font-mono break-all">{error}</p>
            <p className="text-outline text-xs mt-3">
              Make sure the pipeline backend is running (<code className="font-mono">python server.py</code>) and that{' '}
              <code className="font-mono">data/processed/*.parquet</code> exist.
            </p>
          </div>
        )}

        {data && !error && (
          <>
            {/* Level 1: Headline KPIs (F-pattern scan, same hierarchy as the console) */}
            <section aria-label="Key performance indicators">
              <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-6 gap-3">
                <KpiCard
                  label="Listings" value={fmtInt(kpis.total_listings)}
                  sub="clean_listings table" icon={<Package className="w-3.5 h-3.5" />} accent
                />
                <KpiCard
                  label="Products" value={fmtInt(kpis.unique_products)}
                  sub="entity-resolved clusters" icon={<Tag className="w-3.5 h-3.5" />}
                />
                <KpiCard
                  label="Avg price" value={fmtMoney(kpis.avg_price)}
                  sub={`median ${fmtMoney(kpis.median_price)}`} icon={<DollarSign className="w-3.5 h-3.5" />} accent
                />
                <KpiCard
                  label="Repost rate" value={`${kpis.repost_rate_pct ?? '—'}%`}
                  sub={`${fmtInt(kpis.repost_count)} reposts`} icon={<Repeat className="w-3.5 h-3.5" />}
                />
                <KpiCard
                  label="Listings / product" value={String(kpis.listings_per_product ?? '—')}
                  sub={`largest cluster ${fmtInt(kpis.largest_cluster)}`} icon={<Layers className="w-3.5 h-3.5" />}
                />
                <KpiCard
                  label="Resale cycle" value={`${kpis.median_resale_days ?? '—'}d`}
                  sub={`avg ${kpis.avg_resale_days ?? '—'} days`} icon={<Clock className="w-3.5 h-3.5" />}
                />
              </div>
            </section>

            {/* Level 2: Charts */}
            <section id="charts" aria-label="Charts" className="space-y-4">
              <div className="flex items-center gap-2">
                <LineChart className="w-4 h-4 text-outline" />
                <h2 className="text-sm font-semibold text-foreground">Marketplace charts</h2>
                {data.tables && (
                  <span className="text-[10px] font-mono text-outline ml-2">
                    {Object.entries(data.tables).map(([k, v]) => `${k}:${fmtInt(v)}`).join('  ·  ')}
                  </span>
                )}
              </div>
              {data.data_coverage && (
                <p className="text-[10px] font-mono text-outline border border-outline-variant/60 bg-surface-lowest rounded p-2.5">
                  data coverage: {data.data_coverage.synthetic_share_pct}% of listings (the synthetic source) carry dates,
                  cities and seller ids — the real Mercari rows do not. Charts marked "synthetic source only" describe
                  that slice; all other charts cover the full corpus.
                </p>
              )}

              <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
                <ChartCard title="Listings per product" icon={<Layers className="w-3.5 h-3.5" />} subtitle="entity cluster sizes">
                  <SimpleBarChart data={charts.cluster_sizes ?? []} color="var(--chart-secondary)" />
                </ChartCard>
                <ChartCard
                  title="Price drift by listing age" icon={<TrendingUp className="w-3.5 h-3.5" />}
                  subtitle="% vs launch-month price (dated rows)"
                >
                  <SimpleLineChart data={charts.depreciation_curve ?? []} zeroLine formatValue={(v) => `${v > 0 ? '+' : ''}${v.toFixed(1)}%`} />
                </ChartCard>
                <ChartCard title="Listings by platform" icon={<BarChart3 className="w-3.5 h-3.5" />}>
                  <SimpleDonut data={charts.listings_by_platform ?? []} centerLabel="listings" />
                </ChartCard>

                <ChartCard title="Average price by category" icon={<DollarSign className="w-3.5 h-3.5" />} subtitle="top 8 by volume">
                  <SimpleBarChart
                    data={charts.price_by_category ?? []}
                    formatValue={(v) => money(v)}
                    formatLabel={(p) => `${p.label}: ${money(p.value ?? 0)} avg · ${(p.extra ?? 0).toLocaleString()} listings`}
                  />
                </ChartCard>
                <ChartCard title="Price distribution" icon={<DollarSign className="w-3.5 h-3.5" />} subtitle="listings per price band">
                  <SimpleBarChart data={charts.price_bands ?? []} />
                </ChartCard>
                <ChartCard title="Condition mix" icon={<Tag className="w-3.5 h-3.5" />} subtitle="item_condition_id">
                  <SimpleDonut data={charts.condition_mix ?? []} centerLabel="listings" />
                </ChartCard>

                <ChartCard title="Top brands" icon={<Tag className="w-3.5 h-3.5" />} subtitle="volume · avg price" className="lg:col-span-2">
                  <HorizontalBars data={charts.top_brands ?? []} />
                </ChartCard>
                <ChartCard title="Top cities" icon={<MapPin className="w-3.5 h-3.5" />} subtitle="synthetic source only">
                  <HorizontalBars data={charts.top_cities ?? []} color="var(--chart-tertiary)" />
                </ChartCard>

                <ChartCard title="Repost rate by category" icon={<Repeat className="w-3.5 h-3.5" />} subtitle="% of listings flagged" className="lg:col-span-2">
                  <HorizontalBars data={charts.reposts_by_category ?? []} color="var(--chart-zero, #d9534f)" valueSuffix="%" />
                </ChartCard>

                <ChartCard title="Resale speed" icon={<Clock className="w-3.5 h-3.5" />} subtitle="products by avg days-to-resale (delisted rows)">
                  <SimpleBarChart data={charts.resale_speed ?? []} color="var(--chart-secondary)" />
                </ChartCard>
                <ChartCard title="Regional volume" icon={<MapPin className="w-3.5 h-3.5" />} subtitle="items · avg price">
                  <HorizontalBars data={charts.regional_variance ?? []} color="var(--chart-secondary)" />
                </ChartCard>
                <ChartCard title="Listings by month" icon={<CalendarDays className="w-3.5 h-3.5" />} subtitle="synthetic source only" className="lg:col-span-2">
                  <SimpleBarChart data={charts.listings_by_month ?? []} color="var(--chart-tertiary)" />
                </ChartCard>
              </div>
            </section>

            {/* Level 3: Insights — bottom of the page, as requested */}
            <section id="insights" aria-label="Insights" className="space-y-4">
              <div className="flex items-center gap-2">
                <TrendingUp className="w-4 h-4 text-outline" />
                <h2 className="text-sm font-semibold text-foreground">Insights</h2>
                <span className="text-[10px] font-mono text-outline ml-2">
                  auto-generated from the aggregates above
                </span>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
                {(data.insights ?? []).map((ins) => (
                  <div key={ins.title} className="bg-surface-container border border-outline-variant rounded p-4">
                    <div className="flex items-start gap-2.5">
                      <InsightIcon icon={ins.icon} />
                      <div className="min-w-0">
                        <h3 className="text-xs font-semibold text-foreground mb-1">{ins.title}</h3>
                        <p className="text-[11px] leading-relaxed text-outline">{ins.body}</p>
                      </div>
                    </div>
                  </div>
                ))}
                {!(data.insights ?? []).length && (
                  <div className="text-xs text-outline italic border border-dashed border-outline-variant rounded p-4">
                    No insights generated for this dataset.
                  </div>
                )}
              </div>
            </section>

            <footer className="border-t border-outline-variant pt-3 text-center text-xs font-mono text-outline">
              ResellRadar Analytics &middot; source: processed parquet zones (clean / entity_resolved / curated) &middot;
              recomputes every {Math.round((data.ttl_seconds ?? 300) / 60)} min
            </footer>
          </>
        )}
      </main>
    </div>
  );
}
