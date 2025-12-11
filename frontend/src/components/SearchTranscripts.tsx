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

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-2xl font-bold mb-4">Search Transcripts</h2>
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
            placeholder="Enter search text..."
            className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>

        <div className="grid grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Top K Results
            </label>
            <input
              type="number"
              value={topK}
              onChange={(e) => setTopK(Math.max(1, parseInt(e.target.value) || 1))}
              min="1"
              max="50"
              disabled={loading}
              className="w-full px-4 py-2 border border-gray-300 rounded-lg"
            />
          </div>
          <div className="flex items-end">
            <button
              type="submit"
              disabled={loading}
              className="w-full bg-green-600 text-white py-2 rounded-lg font-medium hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed"
            >
              {loading ? 'Searching...' : 'Search'}
            </button>
          </div>
        </div>
      </form>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-800 px-4 py-3 rounded mb-6">
          {error}
        </div>
      )}

      {results.length > 0 && (
        <div className="space-y-4">
          <h3 className="text-lg font-semibold">{results.length} Results Found</h3>
          {results.map((result, index) => (
            <div key={index} className="border border-gray-200 rounded-lg p-4 bg-gray-50">
              <div className="flex justify-between items-start mb-2">
                <span className="font-semibold text-sm text-gray-600">Result {index + 1}</span>
                {result.score !== undefined && (
                  <span className="text-sm bg-blue-100 text-blue-800 px-2 py-1 rounded">
                    Score: {result.score.toFixed(3)}
                  </span>
                )}
                {result.distance !== undefined && (
                  <span className="text-sm bg-purple-100 text-purple-800 px-2 py-1 rounded">
                    Distance: {result.distance.toFixed(3)}
                  </span>
                )}
              </div>
              <p className="text-gray-800 mb-2">{result.text}</p>
              <div className="text-sm text-gray-600 space-y-1">
                <p>
                  <span className="font-medium">Timestamp:</span> {result.meta.start.toFixed(2)}s -{' '}
                  {result.meta.end.toFixed(2)}s
                </p>
                {result.id && <p><span className="font-medium">ID:</span> {result.id}</p>}
              </div>
            </div>
          ))}
        </div>
      )}

      {!loading && results.length === 0 && !error && query && (
        <div className="text-center text-gray-500 py-8">No results found</div>
      )}
    </div>
  );
}
