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

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-2xl font-bold mb-4">Create Clip</h2>
      <form onSubmit={handleSubmit} className="space-y-4 mb-6">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Search Query
          </label>
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            disabled={loading}
            placeholder="What scene do you want to clip?"
            className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Padding (seconds)
          </label>
          <input
            type="number"
            value={padding}
            onChange={(e) => setPadding(parseFloat(e.target.value) || 0)}
            min="0"
            max="60"
            step="0.5"
            disabled={loading}
            className="w-full px-4 py-2 border border-gray-300 rounded-lg"
          />
          <p className="text-xs text-gray-500 mt-1">Add extra seconds before and after the clip</p>
        </div>

        {error && (
          <div className="bg-red-50 border border-red-200 text-red-800 px-4 py-3 rounded">
            {error}
          </div>
        )}

        {success && (
          <div className="bg-green-50 border border-green-200 text-green-800 px-4 py-3 rounded space-y-2">
            <p className="font-semibold">Clip Created Successfully!</p>
            <p className="text-sm">Clip Job ID: {success.clip_job_id}</p>
            <p className="text-sm">
              Duration: {success.clip_start.toFixed(2)}s - {success.clip_end.toFixed(2)}s
            </p>
          </div>
        )}

        <button
          type="submit"
          disabled={loading}
          className="w-full bg-purple-600 text-white py-2 rounded-lg font-medium hover:bg-purple-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
        >
          {loading ? 'Creating Clip...' : 'Create Clip'}
        </button>
      </form>
    </div>
  );
}
