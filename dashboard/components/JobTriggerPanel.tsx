"use client";

import React, { useState } from 'react';
import { Play, Square, Settings, Cpu, Layers } from 'lucide-react';

interface JobTriggerPanelProps {
  isRunning: boolean;
  onStart: (category: string, target: number, threads: number) => void;
  onStop: () => void;
}

export const JobTriggerPanel: React.FC<JobTriggerPanelProps> = ({ isRunning, onStart, onStop }) => {
  const [category, setCategory] = useState<string>('all');
  const [target, setTarget] = useState<number>(500);
  const [threads, setThreads] = useState<number>(16);

  return (
    <div id="controller" className="bg-surface-container border border-outline-variant p-5 rounded flex flex-col justify-between h-full">
      <div>
        <div className="flex justify-between items-center mb-5 pb-3 border-b border-outline-variant">
          <div className="flex items-center gap-2">
            <Settings className="w-4 h-4 text-outline" />
            <h2 className="text-sm font-semibold text-foreground">Job controller</h2>
          </div>
          <span className={`px-2 py-0.5 text-[11px] rounded-sm border ${
            isRunning
              ? 'bg-secondary/10 border-secondary/40 text-secondary'
              : 'bg-surface-high border-outline-variant text-outline'
          }`}>
            {isRunning ? 'Active' : 'Standby'}
          </span>
        </div>

        <div className="mb-5">
          <label className="text-xs text-outline block mb-2 flex items-center gap-1.5">
            <Layers className="w-3.5 h-3.5" />
            Target category
          </label>
          <div className="grid grid-cols-3 gap-2">
            {[
              { id: 'phones', label: 'Phones' },
              { id: 'furniture', label: 'Furniture' },
              { id: 'all', label: 'Both' },
            ].map((cat) => (
              <button
                key={cat.id}
                disabled={isRunning}
                onClick={() => setCategory(cat.id)}
                className={`py-2 px-3 text-xs rounded-sm border transition-colors duration-150 active:scale-[0.97] ${
                  category === cat.id
                    ? 'bg-primary/15 border-primary/60 text-primary font-medium'
                    : 'bg-surface-lowest border-outline-variant text-outline hover:text-foreground hover:border-outline'
                } ${isRunning ? 'opacity-50 cursor-not-allowed' : ''}`}
              >
                {cat.label}
              </button>
            ))}
          </div>
        </div>

        <div className="mb-5">
          <label className="text-xs text-outline block mb-2">Batch target volume</label>
          <div className="flex gap-2 mb-2">
            {[500, 5000, 50000].map((val) => (
              <button
                key={val}
                disabled={isRunning}
                onClick={() => setTarget(val)}
                className={`py-1 px-2.5 text-xs rounded-sm border transition-colors duration-150 active:scale-[0.97] ${
                  target === val
                    ? 'bg-primary border-primary text-surface-lowest font-semibold'
                    : 'bg-surface-lowest border-outline-variant text-outline hover:text-foreground'
                }`}
              >
                {val >= 1000 ? `${val / 1000}k` : val}{val === 50000 ? ' max' : ''}
              </button>
            ))}
          </div>
          <input
            type="number"
            disabled={isRunning}
            value={target}
            onChange={(e) => setTarget(Math.max(10, parseInt(e.target.value) || 500))}
            className="w-full bg-surface-lowest border border-outline-variant px-3 py-1.5 text-xs font-mono text-foreground focus:outline-none focus:border-primary rounded-sm transition-colors duration-150"
          />
        </div>

        <div className="mb-2">
          <div className="flex justify-between items-center mb-2">
            <label className="text-xs text-outline flex items-center gap-1.5">
              <Cpu className="w-3.5 h-3.5" />
              Worker threads
            </label>
            <span className="text-xs font-mono text-primary font-semibold">{threads}</span>
          </div>
          <input
            type="range"
            min="1"
            max="32"
            disabled={isRunning}
            value={threads}
            onChange={(e) => setThreads(parseInt(e.target.value))}
            className="w-full accent-primary bg-surface-lowest h-1.5 rounded-sm cursor-pointer"
          />
        </div>
      </div>

      <div className="pt-4 border-t border-outline-variant flex gap-3">
        {!isRunning ? (
          <button
            onClick={() => onStart(category, target, threads)}
            className="flex-1 bg-primary text-surface-lowest font-semibold text-sm py-2.5 px-4 flex items-center justify-center gap-2 rounded transition-all duration-150 hover:bg-primary-dim active:scale-[0.97]"
          >
            <Play className="w-4 h-4 fill-surface-lowest" />
            <span>Start pipeline</span>
          </button>
        ) : (
          <button
            onClick={onStop}
            className="flex-1 bg-accent-crimson/10 border border-accent-crimson/60 text-accent-crimson font-semibold text-sm py-2.5 px-4 flex items-center justify-center gap-2 rounded transition-all duration-150 hover:bg-accent-crimson/20 active:scale-[0.97]"
          >
            <Square className="w-4 h-4 fill-accent-crimson" />
            <span>Stop pipeline</span>
          </button>
        )}
      </div>
    </div>
  );
};
