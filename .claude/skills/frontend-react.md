# skill: frontend-react
# Trigger: "React", "component", "dashboard", "ECharts", "chart", "UI", "frontend",
#          "Tailwind", "Zustand", "hook", "useState", "useEffect", "rrweb",
#          "session recording", "session playback", "analytics dashboard",
#          "TypeScript", "responsive", "form", "table", "modal"

## Purpose
React frontend patterns for Core&Outline's analytics dashboards, ECharts
visualizations, Zustand state management, and the rrweb session recording
playback UI.

## Stack
- React 18 (functional components + hooks only)
- TypeScript
- Tailwind CSS (utility-first styling)
- Apache ECharts + echarts-for-react (data visualization)
- Zustand (global state)
- React Query / TanStack Query (server state, caching)
- Axios (HTTP client)
- rrweb-player (session recording playback)

---

## Component Template

```tsx
// components/analytics/MetricCard.tsx
import React from "react";

interface MetricCardProps {
  title: string;
  value: string | number;
  change?: number;         // percentage change vs prior period
  changeLabel?: string;    // e.g. "vs last month"
  loading?: boolean;
  onClick?: () => void;
}

export const MetricCard: React.FC<MetricCardProps> = ({
  title,
  value,
  change,
  changeLabel = "vs last month",
  loading = false,
  onClick,
}) => {
  const isPositive = (change ?? 0) >= 0;

  if (loading) {
    return (
      <div className="bg-white rounded-xl p-6 shadow-sm border border-gray-100 animate-pulse">
        <div className="h-4 bg-gray-200 rounded w-1/2 mb-3" />
        <div className="h-8 bg-gray-200 rounded w-3/4" />
      </div>
    );
  }

  return (
    <div
      className={`bg-white rounded-xl p-6 shadow-sm border border-gray-100 transition-shadow
        ${onClick ? "cursor-pointer hover:shadow-md" : ""}`}
      onClick={onClick}
    >
      <p className="text-sm font-medium text-gray-500 mb-1">{title}</p>
      <p className="text-2xl font-bold text-gray-900">{value}</p>
      {change !== undefined && (
        <div className="flex items-center mt-2 gap-1">
          <span
            className={`text-sm font-medium ${
              isPositive ? "text-emerald-600" : "text-red-500"
            }`}
          >
            {isPositive ? "+" : ""}{change.toFixed(1)}%
          </span>
          <span className="text-xs text-gray-400">{changeLabel}</span>
        </div>
      )}
    </div>
  );
};
```

---

## ECharts Dashboard Components

```tsx
// components/charts/MRRTrendChart.tsx
"""
MRR trend chart with waterfall breakdown.
Shows new, expansion, contraction, churn components.
"""

import React from "react";
import ReactECharts from "echarts-for-react";
import type { EChartsOption } from "echarts";

interface MRRData {
  months: string[];
  new_mrr: number[];
  expansion: number[];
  contraction: number[];
  churn: number[];
}

export const MRRTrendChart: React.FC<{ data: MRRData; height?: string }> = ({
  data,
  height = "350px",
}) => {
  const option: EChartsOption = {
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" },
      formatter: (params: any) => {
        const total = params.reduce((sum: number, p: any) => sum + (p.value || 0), 0);
        const rows = params.map(
          (p: any) => `<div style="display:flex;justify-content:space-between;gap:16px">
            <span>${p.marker} ${p.seriesName}</span>
            <span><b>KES ${p.value?.toLocaleString()}</b></span>
          </div>`
        );
        return `<div>${params[0].name}<br/>${rows.join("")}<hr/><b>Net: KES ${total.toLocaleString()}</b></div>`;
      },
    },
    legend: { bottom: 0, data: ["New MRR", "Expansion", "Contraction", "Churn"] },
    grid: { left: "3%", right: "4%", bottom: "10%", top: "5%", containLabel: true },
    xAxis: {
      type: "category",
      data: data.months,
      axisLabel: { fontSize: 11 },
    },
    yAxis: {
      type: "value",
      axisLabel: {
        formatter: (val: number) => `KES ${(val / 1000).toFixed(0)}K`,
        fontSize: 11,
      },
    },
    series: [
      {
        name: "New MRR",
        type: "bar",
        stack: "mrr",
        data: data.new_mrr,
        itemStyle: { color: "#10b981" },
      },
      {
        name: "Expansion",
        type: "bar",
        stack: "mrr",
        data: data.expansion,
        itemStyle: { color: "#34d399" },
      },
      {
        name: "Contraction",
        type: "bar",
        stack: "mrr",
        data: data.contraction.map((v) => -v),
        itemStyle: { color: "#fbbf24" },
      },
      {
        name: "Churn",
        type: "bar",
        stack: "mrr",
        data: data.churn.map((v) => -v),
        itemStyle: { color: "#ef4444" },
      },
    ],
  };

  return <ReactECharts option={option} style={{ height }} notMerge />;
};
```

```tsx
// components/charts/ChurnCohortHeatmap.tsx
import React from "react";
import ReactECharts from "echarts-for-react";

interface CohortData {
  cohorts: string[];    // row labels (cohort months)
  periods: string[];    // column labels (period 0, 1, 2, ...)
  values: (number | null)[][];   // retention rates [0,1]
}

export const ChurnCohortHeatmap: React.FC<{ data: CohortData }> = ({ data }) => {
  const heatmapData: [number, number, number][] = [];
  data.values.forEach((row, i) => {
    row.forEach((val, j) => {
      if (val !== null) heatmapData.push([j, i, val]);
    });
  });

  const option = {
    tooltip: {
      formatter: (p: any) =>
        `${data.cohorts[p.value[1]]} | Period ${p.value[0]}<br/>Retention: ${(p.value[2] * 100).toFixed(1)}%`,
    },
    grid: { left: "12%", right: "8%", top: "3%", bottom: "12%" },
    xAxis: {
      type: "category",
      data: data.periods,
      name: "Period",
      nameLocation: "middle",
      nameGap: 25,
    },
    yAxis: { type: "category", data: data.cohorts },
    visualMap: {
      min: 0, max: 1,
      calculable: true,
      orient: "horizontal",
      left: "center",
      bottom: 0,
      inRange: { color: ["#fef2f2", "#dcfce7"] },  // red (0%) → green (100%)
    },
    series: [{
      type: "heatmap",
      data: heatmapData,
      label: {
        show: true,
        formatter: (p: any) => `${(p.value[2] * 100).toFixed(0)}%`,
        fontSize: 10,
      },
    }],
  };

  return <ReactECharts option={option} style={{ height: "400px" }} notMerge />;
};
```

---

## Zustand Store

```tsx
// store/analyticsStore.ts
import { create } from "zustand";
import { devtools, persist } from "zustand/middleware";
import axios from "axios";

interface DateRange {
  start: string;
  end: string;
}

interface Metrics {
  mrr: number;
  churn_rate: number;
  ltv: number;
  cac: number;
  active_customers: number;
}

interface AnalyticsState {
  businessId: string | null;
  dateRange: DateRange;
  metrics: Metrics | null;
  loading: boolean;
  error: string | null;

  // Actions
  setBusinessId: (id: string) => void;
  setDateRange: (range: DateRange) => void;
  fetchMetrics: () => Promise<void>;
  reset: () => void;
}

export const useAnalyticsStore = create<AnalyticsState>()(
  devtools(
    persist(
      (set, get) => ({
        businessId: null,
        dateRange: {
          start: new Date(Date.now() - 30 * 86400000).toISOString().slice(0, 10),
          end: new Date().toISOString().slice(0, 10),
        },
        metrics: null,
        loading: false,
        error: null,

        setBusinessId: (id) => set({ businessId: id }),
        setDateRange: (range) => set({ dateRange: range }),

        fetchMetrics: async () => {
          const { businessId, dateRange } = get();
          if (!businessId) return;
          set({ loading: true, error: null });
          try {
            const { data } = await axios.get(`/api/v1/metrics/${businessId}`, {
              params: dateRange,
            });
            set({ metrics: data, loading: false });
          } catch (err: any) {
            set({ error: err.message, loading: false });
          }
        },

        reset: () => set({ metrics: null, error: null }),
      }),
      { name: "analytics-store", partialize: (s) => ({ businessId: s.businessId }) }
    )
  )
);
```

---

## Custom Hooks

```tsx
// hooks/useMetrics.ts
import { useQuery } from "@tanstack/react-query";
import axios from "axios";

export const useMetrics = (businessId: string, dateRange: { start: string; end: string }) => {
  return useQuery({
    queryKey: ["metrics", businessId, dateRange],
    queryFn: async () => {
      const { data } = await axios.get(`/api/v1/metrics/${businessId}`, {
        params: dateRange,
      });
      return data;
    },
    staleTime: 5 * 60 * 1000,       // 5 min cache — metrics don't update that often
    retry: 2,
    enabled: !!businessId,
  });
};


// hooks/useStreamingAnalyst.ts
"""Hook for Server-Sent Events from the AI Analyst endpoint."""

import { useState, useCallback } from "react";

export const useStreamingAnalyst = (businessId: string) => {
  const [response, setResponse] = useState("");
  const [isStreaming, setIsStreaming] = useState(false);

  const ask = useCallback(async (question: string) => {
    setResponse("");
    setIsStreaming(true);

    const eventSource = new EventSource(
      `/api/ai/analyst/query?question=${encodeURIComponent(question)}&business_id=${businessId}`
    );

    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === "text") {
        setResponse((prev) => prev + data.content);
      } else if (data.type === "done") {
        eventSource.close();
        setIsStreaming(false);
      }
    };

    eventSource.onerror = () => {
      eventSource.close();
      setIsStreaming(false);
    };

    return () => eventSource.close();
  }, [businessId]);

  return { response, isStreaming, ask };
};
```

---

## rrweb Session Recording Playback

```tsx
// components/sessions/SessionPlayer.tsx
"""
rrweb session recording playback component for Core&Outline's
session recording analysis pipeline.
"""

import React, { useEffect, useRef } from "react";
import rrwebPlayer from "rrweb-player";
import "rrweb-player/dist/style.css";

interface SessionPlayerProps {
  events: object[];        // rrweb event array
  width?: number;
  height?: number;
  autoPlay?: boolean;
  onFinish?: () => void;
}

export const SessionPlayer: React.FC<SessionPlayerProps> = ({
  events,
  width = 1024,
  height = 576,
  autoPlay = false,
  onFinish,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const playerRef = useRef<rrwebPlayer | null>(null);

  useEffect(() => {
    if (!containerRef.current || !events.length) return;

    playerRef.current = new rrwebPlayer({
      target: containerRef.current,
      props: {
        events,
        width,
        height,
        autoPlay,
        mouseTail: { duration: 500, lineCap: "round", lineWidth: 3, strokeStyle: "#ef4444" },
        insertStyleRules: ["* { cursor: default !important; }"],
      },
    });

    if (onFinish) {
      playerRef.current.$on("finish", onFinish);
    }

    return () => {
      playerRef.current?.$destroy();
    };
  }, [events]);

  return (
    <div className="rounded-lg overflow-hidden border border-gray-200">
      <div ref={containerRef} />
    </div>
  );
};
```

---

## Data Table Component

```tsx
// components/ui/DataTable.tsx
import React, { useState } from "react";

interface Column<T> {
  key: keyof T;
  header: string;
  render?: (value: T[keyof T], row: T) => React.ReactNode;
  sortable?: boolean;
  width?: string;
}

interface DataTableProps<T> {
  columns: Column<T>[];
  data: T[];
  pageSize?: number;
  loading?: boolean;
  onRowClick?: (row: T) => void;
}

export function DataTable<T extends { id?: string | number }>({
  columns,
  data,
  pageSize = 20,
  loading = false,
  onRowClick,
}: DataTableProps<T>) {
  const [page, setPage] = useState(0);
  const [sortKey, setSortKey] = useState<keyof T | null>(null);
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");

  const sorted = sortKey
    ? [...data].sort((a, b) => {
        const av = a[sortKey], bv = b[sortKey];
        const cmp = av < bv ? -1 : av > bv ? 1 : 0;
        return sortDir === "asc" ? cmp : -cmp;
      })
    : data;

  const paginated = sorted.slice(page * pageSize, (page + 1) * pageSize);
  const totalPages = Math.ceil(data.length / pageSize);

  const handleSort = (key: keyof T) => {
    if (sortKey === key) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else { setSortKey(key); setSortDir("asc"); }
  };

  if (loading) {
    return (
      <div className="animate-pulse space-y-2">
        {Array.from({ length: 5 }).map((_, i) => (
          <div key={i} className="h-10 bg-gray-100 rounded" />
        ))}
      </div>
    );
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200">
      <table className="w-full text-sm">
        <thead className="bg-gray-50 border-b border-gray-200">
          <tr>
            {columns.map((col) => (
              <th
                key={String(col.key)}
                className={`px-4 py-3 text-left font-medium text-gray-600
                  ${col.sortable ? "cursor-pointer select-none hover:text-gray-900" : ""}
                  ${col.width ?? ""}`}
                onClick={() => col.sortable && handleSort(col.key)}
              >
                <div className="flex items-center gap-1">
                  {col.header}
                  {col.sortable && sortKey === col.key && (
                    <span>{sortDir === "asc" ? "↑" : "↓"}</span>
                  )}
                </div>
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {paginated.map((row, i) => (
            <tr
              key={row.id ?? i}
              className={`${onRowClick ? "cursor-pointer hover:bg-gray-50" : ""}`}
              onClick={() => onRowClick?.(row)}
            >
              {columns.map((col) => (
                <td key={String(col.key)} className="px-4 py-3 text-gray-800">
                  {col.render ? col.render(row[col.key], row) : String(row[col.key] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {totalPages > 1 && (
        <div className="flex justify-between items-center px-4 py-3 border-t border-gray-100">
          <span className="text-xs text-gray-500">
            {page * pageSize + 1}–{Math.min((page + 1) * pageSize, data.length)} of {data.length}
          </span>
          <div className="flex gap-2">
            <button
              className="px-3 py-1 text-xs border rounded disabled:opacity-40"
              disabled={page === 0}
              onClick={() => setPage((p) => p - 1)}
            >
              Previous
            </button>
            <button
              className="px-3 py-1 text-xs border rounded disabled:opacity-40"
              disabled={page === totalPages - 1}
              onClick={() => setPage((p) => p + 1)}
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
```

---

## Usage in Claude Code

```bash
# Create new React component
# Tell Claude: "create a [component name] component for [purpose]"
# Claude will use this skill to scaffold it correctly

# Start dev server on non-default port (common issue)
PORT=3001 npm start

# Build for production
npm run build

# Run TypeScript type check
npx tsc --noEmit

# Analyze bundle size
npm run build -- --stats && npx webpack-bundle-analyzer build/bundle-stats.json
```
