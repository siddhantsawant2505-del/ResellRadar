"use client";

import React, { useState } from 'react';
import { Table, Search, Eye, X, Filter } from 'lucide-react';

interface ListingItem {
  listing_id: string;
  title: string;
  description: string;
  price: number;
  currency: string;
  category: string;
  sub_category: string;
  location_city: string;
  location_region: string;
  posted_date: string;
  delisted_date: string | null;
  seller_type: string;
  source_platform: string;
  scraped_at: string;
}

interface RawDataGridProps {
  items: ListingItem[];
  total: number;
  page: number;
  onPageChange: (newPage: number) => void;
  search: string;
  onSearchChange: (q: string) => void;
  categoryFilter: string;
  onCategoryFilterChange: (cat: string) => void;
}

export const RawDataGrid: React.FC<RawDataGridProps> = ({
  items,
  total,
  page,
  onPageChange,
  search,
  onSearchChange,
  categoryFilter,
  onCategoryFilterChange,
}) => {
  const [selectedItem, setSelectedItem] = useState<ListingItem | null>(null);

  const getPlatformBadge = (platform: string) => {
    switch (platform.toLowerCase()) {
      case 'craigslist':
        return 'bg-purple-950/60 border-purple-500/40 text-purple-300';
      case 'facebook marketplace':
        return 'bg-blue-950/60 border-blue-500/40 text-blue-300';
      case 'offerup':
        return 'bg-emerald-950/60 border-emerald-500/40 text-emerald-300';
      case 'ebay refurbished':
        return 'bg-amber-950/60 border-amber-500/40 text-amber-300';
      default:
        return 'bg-surface-high border-outline-variant text-outline';
    }
  };

  return (
    <div id="stream" className="bg-surface-container border border-outline-variant rounded p-5">
      <div className="flex flex-wrap justify-between items-center mb-4 gap-4 pb-3 border-b border-outline-variant">
        <div className="flex items-center gap-2">
          <Table className="w-4 h-4 text-outline" />
          <h2 className="text-sm font-semibold text-foreground">
            Raw ingestion stream
            <span className="ml-2 text-xs font-normal text-outline font-mono">{total.toLocaleString()} records</span>
          </h2>
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <div className="relative">
            <Search className="w-3.5 h-3.5 text-outline absolute left-2.5 top-2" />
            <input
              type="text"
              placeholder="Search title, city, ID…"
              value={search}
              onChange={(e) => onSearchChange(e.target.value)}
              className="bg-surface-lowest border border-outline-variant pl-8 pr-3 py-1.5 text-xs font-mono text-foreground placeholder-outline focus:outline-none focus:border-primary rounded-sm transition-colors duration-150 w-48 sm:w-60"
            />
          </div>

          <div className="flex items-center gap-1.5 bg-surface-lowest border border-outline-variant px-2 py-1.5 rounded-sm">
            <Filter className="w-3 h-3 text-outline" />
            <select
              value={categoryFilter}
              onChange={(e) => onCategoryFilterChange(e.target.value)}
              className="bg-transparent text-xs text-foreground focus:outline-none cursor-pointer"
            >
              <option value="all" className="bg-surface-container">All categories</option>
              <option value="phones" className="bg-surface-container">Phones &amp; mobile</option>
              <option value="furniture" className="bg-surface-container">Furniture &amp; decor</option>
            </select>
          </div>
        </div>
      </div>

      <div className="overflow-x-auto custom-scrollbar border border-outline-variant rounded-sm">
        <table className="w-full text-left text-xs">
          <thead className="bg-surface-lowest border-b border-outline-variant text-outline text-[11px]">
            <tr>
              <th className="py-2.5 px-3 font-medium">Listing ID / Time</th>
              <th className="py-2.5 px-3 font-medium">Platform</th>
              <th className="py-2.5 px-3 font-medium">Title</th>
              <th className="py-2.5 px-3 font-medium">Category</th>
              <th className="py-2.5 px-3 text-right font-medium">Price</th>
              <th className="py-2.5 px-3 font-medium">Location</th>
              <th className="py-2.5 px-3 font-medium">Seller</th>
              <th className="py-2.5 px-3 text-center font-medium">Inspect</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-outline-variant/30">
            {items.length === 0 ? (
              <tr>
                <td colSpan={8} className="py-10 text-center text-outline text-xs">
                  No listings buffered — start the pipeline to stream data.
                </td>
              </tr>
            ) : (
              items.map((item) => (
                <tr key={item.listing_id} className="hover:bg-surface-high/50 transition-colors duration-100">
                  <td className="py-2 px-3">
                    <div className="text-primary font-mono text-[11px] font-medium">{item.listing_id}</div>
                    <div className="text-[10px] text-outline font-mono">
                      {item.scraped_at ? item.scraped_at.split('T')[1]?.replace('Z', '') : ''}
                    </div>
                  </td>
                  <td className="py-2 px-3">
                    <span className={`px-1.5 py-0.5 border text-[10px] rounded-sm ${getPlatformBadge(item.source_platform)}`}>
                      {item.source_platform}
                    </span>
                  </td>
                  <td className="py-2 px-3 max-w-xs truncate text-foreground" title={item.title}>
                    {item.title}
                  </td>
                  <td className="py-2 px-3">
                    <div className="text-foreground text-[11px]">{item.category}</div>
                    <div className="text-[10px] text-tertiary">{item.sub_category}</div>
                  </td>
                  <td className="py-2 px-3 text-right font-mono font-semibold text-primary text-sm">
                    {typeof item.price === 'number' ? `$${item.price.toFixed(2)}` : '—'}
                  </td>
                  <td className="py-2 px-3 text-outline text-[11px]">
                    {item.location_city}, {item.location_region}
                  </td>
                  <td className="py-2 px-3 text-[11px] text-outline">{item.seller_type}</td>
                  <td className="py-2 px-3 text-center">
                    <button
                      onClick={() => setSelectedItem(item)}
                      className="p-1.5 text-outline hover:text-primary hover:bg-primary/10 rounded transition-colors duration-150 active:scale-[0.97]"
                      title="Inspect raw JSON"
                    >
                      <Eye className="w-3.5 h-3.5" />
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <div className="flex justify-between items-center mt-3 text-xs text-outline">
        <div>
          Showing {items.length} of {total.toLocaleString()} records
        </div>
        <div className="flex items-center gap-1">
          <button
            disabled={page <= 1}
            onClick={() => onPageChange(page - 1)}
            className="px-3 py-1 border border-outline-variant bg-surface-lowest rounded-sm hover:text-foreground hover:border-outline disabled:opacity-40 transition-colors duration-150 active:scale-[0.97]"
          >
            Prev
          </button>
          <span className="px-3 py-1 text-primary font-mono font-semibold">{page}</span>
          <button
            disabled={items.length < 10 || page * 10 >= total}
            onClick={() => onPageChange(page + 1)}
            className="px-3 py-1 border border-outline-variant bg-surface-lowest rounded-sm hover:text-foreground hover:border-outline disabled:opacity-40 transition-colors duration-150 active:scale-[0.97]"
          >
            Next
          </button>
        </div>
      </div>

      {selectedItem && (
        <div
          className="fixed inset-0 z-50 bg-black/70 flex items-center justify-center p-4"
          onClick={() => setSelectedItem(null)}
        >
          <div
            className="modal-enter bg-surface-lowest border border-outline-variant rounded-md w-full max-w-2xl max-h-[80vh] flex flex-col text-xs"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="bg-surface-container px-4 py-3 border-b border-outline-variant flex justify-between items-center rounded-t-md">
              <span className="text-sm font-semibold text-foreground">
                Raw payload <span className="font-mono text-xs text-outline ml-1">{selectedItem.listing_id}</span>
              </span>
              <button
                onClick={() => setSelectedItem(null)}
                className="p-1 text-outline hover:text-foreground rounded transition-colors duration-150 active:scale-[0.97]"
              >
                <X className="w-4 h-4" />
              </button>
            </div>
            <pre className="p-4 overflow-y-auto custom-scrollbar text-secondary text-[11px] leading-relaxed font-mono rounded-b-md">
              {JSON.stringify(selectedItem, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
};
