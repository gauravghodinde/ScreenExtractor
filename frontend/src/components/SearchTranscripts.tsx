'use client';

import { useState, FormEvent } from 'react';
import { searchTranscripts, SearchResult } from '@/lib/api';

interface SearchTranscriptsProps {
  jobId: string;
}

export default function SearchTranscripts({ jobId }: SearchTranscriptsProps) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [topK, setTopK] = useState(5);

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!query.trim()) {
      setError('Please enter a search query');
      return;
    }

    setLoading(true);
    setError(null);

    try {
      const searchResults = await searchTranscripts(query, jobId, topK);
      setResults(searchResults);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Search failed';
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  const formatTime = (seconds: number) => {
    const m = Math.floor(seconds / 60);
    const s = Math.floor(seconds % 60);
    return `${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
  };

  return (
    <div className="glass-card p-6">
      <div className="flex items-center gap-3 mb-5">
        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-emerald-500 to-teal-600 flex items-center justify-center">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
        </div>
        <div>
          <h2 className="text-xl font-bold text-white">Search Transcripts</h2>
          <p className="text-xs text-slate-500 font-mono">Job: {jobId.slice(0, 12)}…</p>
        </div>
      </div>

      <form onSubmit={handleSubmit} className="space-y-4 mb-6">
        <div className="relative">
          <svg className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="11" cy="11" r="8" />
            <line x1="21" y1="21" x2="16.65" y2="16.65" />
          </svg>
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            disabled={loading}
            placeholder="Search dialogue…"
            className="input-dark !pl-10"
          />
        </div>

        <div className="flex gap-3">
          <div className="flex-1">
            <label className="block text-xs font-medium text-slate-500 mb-1.5">Top K</label>
            <input
              type="number"
              value={topK}
              onChange={(e) => setTopK(Math.max(1, parseInt(e.target.value) || 1))}
              min="1"
              max="50"
              disabled={loading}
              className="input-dark"
            />
          </div>
          <div className="flex items-end">
            <button
              type="submit"
              disabled={loading}
              className="btn-gradient flex items-center gap-2 whitespace-nowrap"
            >
              {loading ? (
                <>
                  <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z" />
                  </svg>
                  Searching…
                </>
              ) : (
                'Search'
              )}
            </button>
          </div>
        </div>
      </form>

      {error && <div className="alert-error mb-4">{error}</div>}

      {results.length > 0 && (
        <div className="space-y-3">
          <p className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
            {results.length} Results Found
          </p>
          {results.map((result, index) => (
            <div
              key={index}
              className="bg-white/[0.03] border border-white/5 rounded-xl p-4 hover:border-indigo-500/20 transition-colors"
            >
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs font-mono text-cyan-400">
                  {formatTime(result.meta.start)} → {formatTime(result.meta.end)}
                </span>
                <div className="flex gap-2">
                  {result.score !== undefined && (
                    <span className="badge badge-success">
                      {result.score.toFixed(3)}
                    </span>
                  )}
                  {result.distance !== undefined && (
                    <span className="badge badge-default">
                      d={result.distance.toFixed(3)}
                    </span>
                  )}
                </div>
              </div>
              <p className="text-sm text-slate-300 leading-relaxed">{result.text}</p>
            </div>
          ))}
        </div>
      )}

      {!loading && results.length === 0 && !error && query && (
        <div className="text-center text-slate-500 py-8 text-sm">No results found</div>
      )}
    </div>
  );
}
