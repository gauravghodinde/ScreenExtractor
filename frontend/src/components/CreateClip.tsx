'use client';

import { useState, FormEvent } from 'react';
import { createClip, ClipJobResponse } from '@/lib/api';

interface CreateClipProps {
  jobId: string;
  onSuccess?: (clipJob: ClipJobResponse) => void;
  onError?: (error: string) => void;
}

export default function CreateClip({ jobId, onSuccess, onError }: CreateClipProps) {
  const [query, setQuery] = useState('');
  const [padding, setPadding] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState<ClipJobResponse | null>(null);

  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!query.trim()) {
      setError('Please enter a search query');
      return;
    }

    setLoading(true);
    setError(null);
    setSuccess(null);

    try {
      const result = await createClip(query, jobId, 1, padding);
      setSuccess(result);
      onSuccess?.(result);
      setQuery('');
      setPadding(0);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to create clip';
      setError(message);
      onError?.(message);
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
        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-violet-500 to-purple-600 flex items-center justify-center">
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <rect x="2" y="2" width="20" height="20" rx="2.18" ry="2.18" />
            <line x1="7" y1="2" x2="7" y2="22" />
            <line x1="17" y1="2" x2="17" y2="22" />
            <line x1="2" y1="12" x2="22" y2="12" />
            <line x1="2" y1="7" x2="7" y2="7" />
            <line x1="2" y1="17" x2="7" y2="17" />
            <line x1="17" y1="17" x2="22" y2="17" />
            <line x1="17" y1="7" x2="22" y2="7" />
          </svg>
        </div>
        <div>
          <h2 className="text-xl font-bold text-white">Create Clip</h2>
          <p className="text-xs text-slate-500 font-mono">Job: {jobId.slice(0, 12)}…</p>
        </div>
      </div>

      <form onSubmit={handleSubmit} className="space-y-4 mb-4">
        <div>
          <label className="block text-xs font-medium text-slate-500 mb-1.5">Scene Query</label>
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            disabled={loading}
            placeholder="Describe the scene you want…"
            className="input-dark"
          />
        </div>

        <div>
          <label className="block text-xs font-medium text-slate-500 mb-1.5">Padding (seconds)</label>
          <input
            type="number"
            value={padding}
            onChange={(e) => setPadding(parseFloat(e.target.value) || 0)}
            min="0"
            max="60"
            step="0.5"
            disabled={loading}
            className="input-dark"
          />
          <p className="text-[0.7rem] text-slate-600 mt-1">Extra seconds before &amp; after the matched scene</p>
        </div>

        {error && <div className="alert-error">{error}</div>}

        {success && (
          <div className="alert-success space-y-2">
            <div className="flex items-center gap-2">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <polyline points="20 6 9 17 4 12" />
              </svg>
              <span className="font-semibold">Clip Created!</span>
            </div>
            <p className="text-xs opacity-80">ID: {success.clip_job_id}</p>
            <p className="text-xs opacity-80">
              Duration: {formatTime(success.clip_start)} → {formatTime(success.clip_end)}
            </p>
          </div>
        )}

        <button
          type="submit"
          disabled={loading}
          className="btn-gradient w-full flex items-center justify-center gap-2"
        >
          {loading ? (
            <>
              <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z" />
              </svg>
              Creating Clip…
            </>
          ) : (
            <>
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <rect x="2" y="2" width="20" height="20" rx="2.18" ry="2.18" />
                <line x1="7" y1="2" x2="7" y2="22" />
                <line x1="17" y1="2" x2="17" y2="22" />
              </svg>
              Create Clip
            </>
          )}
        </button>
      </form>
    </div>
  );
}
