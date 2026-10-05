"use client";

import React from 'react';

// Lightweight dependency-free SVG charts matching the console's dark tokens.
// (No chart library in dashboard/package.json — these keep the bundle tiny and
// the user's dev server untouched.)

export interface ChartPoint {
  label: string;
  value: number | null;
  extra?: number;
}

/* ------------------------------ Bar chart ------------------------------ */

export const SimpleBarChart: React.FC<{
  data: ChartPoint[];
  height?: number;
  color?: string;
  formatValue?: (v: number, point: ChartPoint) => string;
  formatLabel?: (p: ChartPoint) => string;
}> = ({ data, height = 200, color = 'var(--chart-primary)', formatValue, formatLabel }) => {
  const pts = data.filter((d) => typeof d.value === 'number');
  if (!pts.length) {
    return <div className="h-[200px] flex items-center justify-center text-xs text-outline italic">No data available</div>;
  }
  const max = Math.max(...pts.map((d) => d.value as number), 1);

  return (
    <div className="flex items-end gap-1.5" style={{ height }}>
      {pts.map((d) => {
        const v = d.value as number;
        const h = Math.max(2, (v / max) * (height - 34));
        return (
          <div key={d.label} className="flex-1 min-w-0 flex flex-col items-center justify-end h-full group relative">
            <span className="text-[10px] font-mono text-outline mb-1 opacity-0 group-hover:opacity-100 transition-opacity absolute -top-0.5 whitespace-nowrap z-10">
              {formatValue ? formatValue(v, d) : v.toLocaleString()}
            </span>
            <span className="text-[10px] font-mono text-foreground/80 mb-1">{formatValue ? formatValue(v, d) : v.toLocaleString()}</span>
            <div
              className="w-full rounded-t transition-all duration-500 group-hover:opacity-80"
              style={{ height: h, backgroundColor: color }}
              title={formatLabel ? formatLabel(d) : `${d.label}: ${v.toLocaleString()}`}
            />
            <span className="text-[9px] font-mono text-outline mt-1.5 truncate w-full text-center" title={d.label}>
              {d.label}
            </span>
          </div>
        );
      })}
    </div>
  );
};

/* --------------------------- Horizontal bars --------------------------- */

export const HorizontalBars: React.FC<{
  data: ChartPoint[];
  color?: string;
  valueSuffix?: string;
}> = ({ data, color = 'var(--chart-primary)', valueSuffix = '' }) => {
  const pts = data.filter((d) => typeof d.value === 'number');
  if (!pts.length) {
    return <div className="h-[180px] flex items-center justify-center text-xs text-outline italic">No data available</div>;
  }
  const max = Math.max(...pts.map((d) => d.value as number), 1);

  return (
    <div className="space-y-2">
      {pts.map((d) => {
        const v = d.value as number;
        return (
          <div key={d.label} className="group">
            <div className="flex justify-between items-center text-[11px] mb-0.5">
              <span className="text-foreground/90 truncate font-mono" title={d.label}>{d.label}</span>
              <span className="text-outline font-mono shrink-0 ml-2">
                {v.toLocaleString()}{valueSuffix}
                {typeof d.extra === 'number' && (
                  <span className="text-foreground/60 ml-1.5">· ${d.extra.toLocaleString(undefined, { maximumFractionDigits: 2 })}</span>
                )}
              </span>
            </div>
            <div className="h-1.5 bg-surface-high rounded-full overflow-hidden">
              <div
                className="h-full rounded-full transition-all duration-500 group-hover:opacity-80"
                style={{ width: `${Math.max(2, (v / max) * 100)}%`, backgroundColor: color }}
                title={`${d.label}: ${v.toLocaleString()}${valueSuffix}`}
              />
            </div>
          </div>
        );
      })}
    </div>
  );
};

/* ------------------------------ Line chart ----------------------------- */

export const SimpleLineChart: React.FC<{
  data: ChartPoint[];
  height?: number;
  color?: string;
  zeroLine?: boolean;
  formatValue?: (v: number) => string;
}> = ({ data, height = 220, color = 'var(--chart-primary)', zeroLine = false, formatValue }) => {
  const pts = data.filter((d) => typeof d.value === 'number') as { label: string; value: number }[];
  if (pts.length < 2) {
    return <div className="h-[220px] flex items-center justify-center text-xs text-outline italic">Not enough data points to plot</div>;
  }
  const W = 600;
  const H = height;
  const padX = 34, padTop = 12, padBottom = 26;
  const values = pts.map((d) => d.value);
  const rawMin = Math.min(0, ...values);
  const rawMax = Math.max(...values);
  const span = rawMax - rawMin || 1;
  const yMin = zeroLine ? rawMin : Math.max(0, rawMin);
  const yMax = rawMax + span * 0.08;

  const x = (i: number) => padX + (i / (pts.length - 1)) * (W - padX - 8);
  const y = (v: number) => padTop + (1 - (v - yMin) / (yMax - yMin)) * (H - padTop - padBottom);

  const line = pts.map((d, i) => `${i === 0 ? 'M' : 'L'}${x(i).toFixed(1)},${y(d.value).toFixed(1)}`).join(' ');
  const area = `${line} L${x(pts.length - 1).toFixed(1)},${y(yMin).toFixed(1)} L${x(0).toFixed(1)},${y(yMin).toFixed(1)} Z`;
  const gridLines = 4;
  const labelEvery = Math.ceil(pts.length / 12);

  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="w-full" style={{ height }} role="img" aria-label="line chart">
      <defs>
        <linearGradient id="lineFill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.25" />
          <stop offset="100%" stopColor={color} stopOpacity="0.02" />
        </linearGradient>
      </defs>
      {Array.from({ length: gridLines + 1 }, (_, i) => {
        const gv = yMin + ((yMax - yMin) * i) / gridLines;
        return (
          <g key={i}>
            <line x1={padX} x2={W - 8} y1={y(gv)} y2={y(gv)} stroke="var(--chart-grid)" strokeWidth="1" />
            <text x={padX - 6} y={y(gv) + 3} textAnchor="end" fontSize="9" fill="var(--chart-text)" fontFamily="monospace">
              {Math.round(gv)}
            </text>
          </g>
        );
      })}
      {zeroLine && yMin < 0 && (
        <line x1={padX} x2={W - 8} y1={y(0)} y2={y(0)} stroke="var(--chart-zero)" strokeWidth="1" strokeDasharray="3 3" />
      )}
      <path d={area} fill="url(#lineFill)" />
      <path d={line} fill="none" stroke={color} strokeWidth="2" strokeLinejoin="round" strokeLinecap="round" />
      {pts.map((d, i) => (
        <g key={d.label}>
          <circle cx={x(i)} cy={y(d.value)} r="2.5" fill={color}>
            <title>{`${d.label}: ${formatValue ? formatValue(d.value) : d.value.toLocaleString()}`}</title>
          </circle>
          {i % labelEvery === 0 && (
            <text x={x(i)} y={H - 8} textAnchor="middle" fontSize="9" fill="var(--chart-text)" fontFamily="monospace">
              {d.label}
            </text>
          )}
        </g>
      ))}
    </svg>
  );
};

/* ------------------------------ Donut ---------------------------------- */

export const SimpleDonut: React.FC<{
  data: ChartPoint[];
  size?: number;
  colors?: string[];
  centerLabel?: string;
  centerValue?: string;
}> = ({ data, size = 170, colors = ['var(--chart-primary)', 'var(--chart-secondary)', 'var(--chart-tertiary)', 'var(--chart-outline)'], centerLabel, centerValue }) => {
  const pts = data.filter((d) => (d.value ?? 0) > 0);
  const total = pts.reduce((s, d) => s + (d.value ?? 0), 0);
  if (!total) {
    return <div className="h-[170px] flex items-center justify-center text-xs text-outline italic">No data available</div>;
  }
  const R = size / 2 - 6;
  const C = 2 * Math.PI * R;
  let offset = 0;

  return (
    <div className="flex items-center gap-5 flex-wrap">
      <div className="relative shrink-0" style={{ width: size, height: size }}>
        <svg width={size} height={size} className="-rotate-90">
          {pts.map((d, i) => {
            const frac = (d.value ?? 0) / total;
            const dash = frac * C;
            const el = (
              <circle
                key={d.label}
                cx={size / 2}
                cy={size / 2}
                r={R}
                fill="none"
                stroke={colors[i % colors.length]}
                strokeWidth="14"
                strokeDasharray={`${dash} ${C - dash}`}
                strokeDashoffset={-offset}
              >
                <title>{`${d.label}: ${(d.value ?? 0).toLocaleString()} (${(frac * 100).toFixed(1)}%)`}</title>
              </circle>
            );
            offset += dash;
            return el;
          })}
        </svg>
        <div className="absolute inset-0 flex flex-col items-center justify-center">
          <span className="text-lg font-semibold text-foreground font-mono">{centerValue ?? total.toLocaleString()}</span>
          <span className="text-[10px] text-outline">{centerLabel ?? 'total'}</span>
        </div>
      </div>
      <div className="space-y-1.5 min-w-0">
        {pts.map((d, i) => (
          <div key={d.label} className="flex items-center gap-2 text-[11px]">
            <span className="w-2.5 h-2.5 rounded-sm shrink-0" style={{ backgroundColor: colors[i % colors.length] }} />
            <span className="text-foreground/90 font-mono truncate">{d.label}</span>
            <span className="text-outline font-mono ml-auto pl-3 shrink-0">
              {((d.value ?? 0) / total * 100).toFixed(1)}%
            </span>
          </div>
        ))}
      </div>
    </div>
  );
};
