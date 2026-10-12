---
name: ResellRadar Ingestion Console
description: Data engineering pipeline monitoring dashboard for resale marketplace data (real + synthetic + manual uploads).
colors:
  primary: "#e8a733"
  primary-container: "#c48820"
  primary-dim: "#f5c86a"
  secondary: "#5cb87a"
  tertiary: "#9b8ff0"
  error: "#d9534f"
  warning: "#c8892a"
  foreground: "#d4cfc9"
  surface: "#13110f"
  surface-lowest: "#0e0d0b"
  surface-container: "#252320"
  surface-high: "#2e2c28"
  outline: "#a09890"
  outline-variant: "#5c5450"
typography:
  heading:
    fontFamily: Inter
    fontSize: 14px
    fontWeight: 600
    lineHeight: 1.4
  body:
    fontFamily: Inter
    fontSize: 13px
    fontWeight: 400
    lineHeight: 1.6
  mono-data:
    fontFamily: JetBrains Mono
    fontSize: 12px
    fontWeight: 400
    lineHeight: 1.5
rounded:
  sm: 2px
  md: 4px
  lg: 6px
spacing:
  xs: 4px
  sm: 8px
  md: 16px
  lg: 24px
  xl: 32px
components:
  button-primary:
    backgroundColor: "{colors.primary}"
    textColor: "{colors.surface}"
    rounded: "{rounded.md}"
    padding: 10px 16px
  button-stop:
    backgroundColor: "transparent"
    textColor: "{colors.error}"
    rounded: "{rounded.md}"
    padding: 10px 16px
  card:
    backgroundColor: "{colors.surface-container}"
    rounded: "{rounded.md}"
    padding: 20px
  badge:
    rounded: "{rounded.sm}"
    padding: 2px 6px
---

# ResellRadar Ingestion Console

## Overview

ResellRadar is a data engineering dashboard for monitoring and analysing a resale-marketplace data platform: real Mercari listings, a seeded synthetic generator, and operator-supplied upload batches flow into an HDFS raw zone and through Apache Spark into processed and curated tables.

The console (`/`) is the operational view: live pipeline telemetry, the job controller (start/stop + batch target), a streaming log terminal, the Spark batch-cluster diagram, the manual-ingestion panel, HDFS sync status, and a raw data grid. The analytics page (`/analytics`) is the read-only view: headline KPIs, marketplace charts, and auto-generated insights from the curated parquet tables.

Audience: data engineers. Emotional tone: precise, reliable, authoritative — like an instrument panel. The interface should feel purpose-built and serious, not decorative.

## Colors

- **Primary (#e8a733 — Signal Amber):** The single most important accent. Used for active states, primary values, CTA buttons, and interactive highlights. Amber is the universal language of instrument displays — reads as "active" without the alarm associations of red or the generic tech-blue of most dashboards.
- **Secondary (#5cb87a — Muted Green):** Success, running states, HDFS connected status. Muted rather than electric.
- **Tertiary (#9b8ff0 — Slate Lavender):** HDFS sync actions, tertiary data signals.
- **Surface (#13110f — Warm Charcoal):** Background. Warm brown undertone, not cold blue-black. Feels like aged instrument casing.
- **Foreground (#d4cfc9):** Primary readable text. Warm off-white.
- **Outline (#a09890):** Muted labels and secondary text. Warm grey.

## Typography

- Inter for all UI labels, headings, and navigational text. Normal case throughout.
- JetBrains Mono only for live numeric data, timestamps, IDs, log content, and code literals.
- No ALL-CAPS, no tracked-out uppercase for headings or nav.

## Layout

- Max width 1700px, centered, with 24px gutters on desktop.
- Anchor nav: Metrics, Controller, Cluster, Upload, HDFS sync, Raw stream (plus a link to the analytics page).
- Four-column KPI strip.
- Two-column row (5/7 split): Job Controller left, Log Terminal right.
- Full-width Batch cluster execution diagram (driver → Spark master → worker-unit cards).
- Full-width Manual ingestion panel (drop target, source label, merge checkbox, recent uploads).
- Full-width HDFS sync panel.
- Full-width data grid at bottom.

## Elevation & Depth

Flat with border distinction. No box shadows. Surface hierarchy via background colors. Top accent stripe on KPI cards (2px colored line) to distinguish card types without heavy decoration.

## Shapes

- `rounded-sm` (2px): badges, inputs, small controls.
- `rounded` (4px): cards, panels, table wrappers.
- `rounded-md` (6px): modal dialogs.

## Components

- **Buttons:** Primary = amber fill, dark text. Stop = error-tinted border, transparent fill. All buttons have `active:scale(0.97)` press feedback.
- **Status indicators:** Opacity-only pulse animation.
- **Log badges:** Lowercase, rounded-sm.
- **Cluster panel:** One card per Spark worker unit, laid out as an execution-flow diagram (driver → Spark master → workers); live CPU % / memory from `docker stats`, a spinner while a stage is executing, and a FINISHED/FAILED ledger for the run's applications.
- **Upload panel:** A dashed-border drop zone (`accept=".csv,.tsv,.json,.jsonl"`, 200 MB cap), optional source label, a "merge now" checkbox (default on, warns when the Spark cluster is down), inline result feedback (accepted / rejected / duplicates), and a list of the last 24 h of upload runs.
- **Modal:** Enters with scale(0.95→1) + opacity, 200ms ease-out.

## Do's and Don'ts

- Do use amber only for the most important interactive elements and live values.
- Do keep monospace strictly for data — never for headings or labels.
- Don't use ALL-CAPS for section labels, nav items, or buttons.
- Don't use numbered prefixes on section headings.
- Don't add glow box-shadows.
- Do maintain WCAG AA contrast on all text/background pairs.
