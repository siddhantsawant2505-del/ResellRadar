"use client";

import React, { useCallback, useEffect, useState } from 'react';
import { UploadCloud, FileText, CheckCircle2, AlertTriangle, Loader2, Database } from 'lucide-react';

interface UploadSummary {
  accepted?: number | null;
  rejected?: number | null;
  duplicates_of_existing?: number | null;
  raw_path?: string;
  source_file?: string;
}

interface UploadRun {
  run: string;
  accepted?: number | null;
  rejected?: number | null;
  duplicates_of_existing?: number | null;
  source_file?: string;
  stored_at?: string;
}

interface UploadPanelProps {
  isRunning: boolean;
  clusterUp: boolean;
  onLogRefresh: () => void;
}

const fmt = (n?: number | null) => (typeof n === 'number' ? n.toLocaleString() : '—');

export const UploadPanel: React.FC<UploadPanelProps> = ({ isRunning, clusterUp, onLogRefresh }) => {
  const [file, setFile] = useState<File | null>(null);
  const [sourceName, setSourceName] = useState('');
  const [mergeNow, setMergeNow] = useState(true);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<{ ok: boolean; text: string } | null>(null);
  const [uploads, setUploads] = useState<UploadRun[]>([]);

  const loadUploads = useCallback(async () => {
    try {
      const res = await fetch('/api/ingest/uploads', { cache: 'no-store' });
      if (res.ok) setUploads((await res.json()).uploads || []);
    } catch {
      /* backend down — panel still usable for the next attempt */
    }
  }, []);

  useEffect(() => {
    loadUploads();
    const t = setInterval(loadUploads, 15000);
    return () => clearInterval(t);
  }, [loadUploads]);

  const submit = async () => {
    if (!file || busy) return;
    setBusy(true);
    setResult(null);
    try {
      const form = new FormData();
      form.append('file', file);
      if (sourceName.trim()) form.append('source_name', sourceName.trim());
      form.append('rerun', mergeNow ? 'true' : 'false');
      const res = await fetch('/api/ingest/upload', { method: 'POST', body: form });
      const json = await res.json();
      if (json.status === 'ERROR' || json.error) {
        setResult({ ok: false, text: json.message || json.error || 'Upload failed' });
      } else if (json.status === 'ACCEPTED') {
        setResult({
          ok: true,
          text: `${fmt(json.summary?.accepted)} rows ingested — pipeline rerunning to merge them. Watch the log terminal.`,
        });
        setFile(null);
        setSourceName('');
        const el = document.getElementById('stream');
        el?.scrollIntoView();
      } else {
        setResult({ ok: true, text: `${fmt(json.summary?.accepted)} rows stored in the raw zone (merge on next run).` });
        setFile(null);
        setSourceName('');
      }
    } catch (err) {
      setResult({ ok: false, text: err instanceof Error ? err.message : String(err) });
    } finally {
      setBusy(false);
      loadUploads();
      onLogRefresh();
    }
  };

  const accent = mergeNow && !clusterUp ? 'border-accent-crimson/60' : 'border-outline-variant';

  return (
    <div id="upload" className="bg-surface-container border border-outline-variant rounded p-5">
      <div className="flex flex-wrap justify-between items-center mb-4 pb-3 border-b border-outline-variant gap-2">
        <div className="flex items-center gap-2">
          <UploadCloud className="w-4 h-4 text-outline" />
          <h2 className="text-sm font-semibold text-foreground">
            Manual ingestion
            <span className="ml-2 text-xs font-normal text-outline font-mono">upload → raw zone → merge</span>
          </h2>
        </div>
        <span className="text-[10px] font-mono text-outline">CSV · TSV · JSON · JSONL · max 200 MB</span>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-4">
        {/* Drop zone + form */}
        <div className="lg:col-span-5 space-y-3">
          <label
            className={`flex flex-col items-center justify-center gap-2 rounded border-2 border-dashed ${accent} bg-surface-lowest px-4 py-6 cursor-pointer hover:border-primary/60 transition-colors ${file ? 'border-primary/70' : ''}`}
          >
            <input
              type="file"
              accept=".csv,.tsv,.json,.jsonl"
              className="hidden"
              onChange={(e) => {
                setFile(e.target.files?.[0] ?? null);
                setResult(null);
              }}
            />
            {busy ? (
              <Loader2 className="w-6 h-6 text-primary animate-spin" />
            ) : (
              <UploadCloud className={`w-6 h-6 ${file ? 'text-primary' : 'text-outline'}`} />
            )}
            <span className="text-xs text-foreground/90 text-center px-2 break-all">
              {file ? file.name : 'Click to choose a data file'}
            </span>
            <span className="text-[10px] text-outline font-mono">
              {file ? `${(file.size / 1024).toFixed(1)} KB` : 'title + price required per row'}
            </span>
          </label>

          <input
            value={sourceName}
            onChange={(e) => setSourceName(e.target.value)}
            placeholder="source label (optional) — e.g. ebay-dump"
            className="w-full bg-surface-lowest border border-outline-variant rounded px-3 py-2 text-xs font-mono text-foreground placeholder:text-outline/70 focus:outline-none focus:border-primary/60"
          />

          <label className="flex items-start gap-2 text-[11px] text-outline cursor-pointer">
            <input
              type="checkbox"
              checked={mergeNow}
              onChange={(e) => setMergeNow(e.target.checked)}
              className="mt-0.5 accent-[#e8a733]"
            />
            <span>
              Merge now — rerun the pipeline after ingest
              {mergeNow && !clusterUp && (
                <span className="text-accent-crimson"> (Spark cluster is down — rerun will fail)</span>
              )}
            </span>
          </label>

          <button
            onClick={submit}
            disabled={!file || busy || (mergeNow && isRunning)}
            className="w-full flex items-center justify-center gap-2 px-4 py-2.5 text-xs font-semibold rounded bg-primary/15 border border-primary/50 text-primary hover:bg-primary/25 transition-colors active:scale-[0.99] disabled:opacity-40 disabled:cursor-not-allowed"
          >
            {busy ? <Loader2 className="w-4 h-4 animate-spin" /> : <UploadCloud className="w-4 h-4" />}
            {mergeNow ? 'Ingest & merge' : 'Ingest to raw zone'}
          </button>

          {result && (
            <div
              className={`flex items-start gap-2 text-[11px] rounded p-2.5 border ${
                result.ok
                  ? 'border-secondary/50 bg-secondary/5 text-foreground'
                  : 'border-accent-crimson/50 bg-accent-crimson/5 text-foreground'
              }`}
            >
              {result.ok ? (
                <CheckCircle2 className="w-3.5 h-3.5 text-secondary shrink-0 mt-0.5" />
              ) : (
                <AlertTriangle className="w-3.5 h-3.5 text-accent-crimson shrink-0 mt-0.5" />
              )}
              <span>{result.text}</span>
            </div>
          )}
        </div>

        {/* Recent uploads */}
        <div className="lg:col-span-7">
          <div className="flex items-center gap-1.5 text-xs text-outline mb-2">
            <Database className="w-3.5 h-3.5" />
            Recent uploads (last 24h)
          </div>
          {uploads.length === 0 ? (
            <div className="text-[11px] text-outline italic border border-dashed border-outline-variant rounded p-3">
              No manual uploads yet — ingested batches appear here with their accepted/rejected counts.
            </div>
          ) : (
            <div className="max-h-[190px] overflow-y-auto custom-scrollbar space-y-1.5">
              {uploads.map((u) => (
                <div
                  key={u.run}
                  className="flex items-center gap-2 text-[11px] font-mono border border-outline-variant/60 bg-surface-lowest rounded p-2"
                >
                  <FileText className="w-3.5 h-3.5 text-outline shrink-0" />
                  <span className="text-foreground/90 truncate">{u.source_file || u.run}</span>
                  <span className="ml-auto shrink-0 text-secondary">{fmt(u.accepted)} accepted</span>
                  {typeof u.rejected === 'number' && u.rejected > 0 && (
                    <span className="shrink-0 text-accent-crimson">{fmt(u.rejected)} rejected</span>
                  )}
                  <span className="shrink-0 text-outline">{(u.stored_at || '').slice(11, 19)}Z</span>
                </div>
              ))}
            </div>
          )}
          <p className="text-[10px] text-outline leading-relaxed mt-2">
            Rows are normalized to the 19-field union schema; listing ids that already exist are skipped, so re-uploads
            never duplicate. Merging recomputes the whole pipeline from the raw zone (the same deterministic full rebuild
            the nightly pipeline does).
          </p>
        </div>
      </div>
    </div>
  );
};
