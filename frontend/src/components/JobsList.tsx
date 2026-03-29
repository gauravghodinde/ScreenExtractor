'use client';

import { useState, useEffect } from 'react';
import { getJobs, Job } from '@/lib/api';

interface JobsListProps {
  onSelectJob?: (jobId: string) => void;
}

export default function JobsList({ onSelectJob }: JobsListProps) {
  const [jobs, setJobs] = useState<Job[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<string>('');

  useEffect(() => {
    const fetchJobs = async () => {
      try {
        const fetchedJobs = await getJobs(20, 0, statusFilter || undefined);
        setJobs(fetchedJobs);
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Failed to fetch jobs';
        setError(message);
      } finally {
        setLoading(false);
      }
    };

    fetchJobs();
  }, [statusFilter]);

  const getStatusBadge = (status: string) => {
    switch (status) {
      case 'done':
      case 'completed':
        return 'badge-success';
      case 'pending':
      case 'processing':
      case 'queued':
        return 'badge-warning';
      case 'failed':
      case 'error':
        return 'badge-error';
      default:
        return 'badge-default';
    }
  };

  const getStatusDot = (status: string) => {
    switch (status) {
      case 'done':
      case 'completed':
        return 'bg-emerald-400';
      case 'pending':
      case 'processing':
      case 'queued':
        return 'bg-amber-400 animate-pulse';
      case 'failed':
      case 'error':
        return 'bg-rose-400';
      default:
        return 'bg-slate-400';
    }
  };

  if (error) {
    return <div className="alert-error">{error}</div>;
  }

  return (
    <div className="glass-card p-6 h-full">
      <div className="flex items-center justify-between mb-5">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center">
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <rect x="2" y="3" width="20" height="14" rx="2" ry="2" />
              <line x1="8" y1="21" x2="16" y2="21" />
              <line x1="12" y1="17" x2="12" y2="21" />
            </svg>
          </div>
          <h2 className="text-xl font-bold text-white">Recent Jobs</h2>
        </div>
        <select
          value={statusFilter}
          onChange={(e) => setStatusFilter(e.target.value)}
          className="select-dark"
        >
          <option value="">All Statuses</option>
          <option value="queued">Queued</option>
          <option value="pending">Pending</option>
          <option value="done">Done</option>
          <option value="failed">Failed</option>
        </select>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-12">
          <svg className="animate-spin h-6 w-6 text-violet-400" viewBox="0 0 24 24" fill="none">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z" />
          </svg>
        </div>
      ) : jobs.length === 0 ? (
        <div className="text-center py-12 text-slate-500 text-sm">No jobs found</div>
      ) : (
        <div className="overflow-x-auto -mx-6 px-6">
          <table className="table-dark">
            <thead>
              <tr>
                <th>Job ID</th>
                <th>Status</th>
                <th>Created</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((job) => (
                <tr key={job.id}>
                  <td>
                    <button
                      onClick={() => onSelectJob?.(job.id)}
                      className="font-mono text-xs text-indigo-400 hover:text-indigo-300 transition-colors cursor-pointer hover:underline underline-offset-2"
                    >
                      {job.id.length > 16 ? `${job.id.slice(0, 8)}…${job.id.slice(-8)}` : job.id}
                    </button>
                  </td>
                  <td>
                    <span className={`badge ${getStatusBadge(job.status)}`}>
                      <span className={`inline-block w-1.5 h-1.5 rounded-full ${getStatusDot(job.status)}`} />
                      {job.status}
                    </span>
                  </td>
                  <td className="text-xs">
                    {new Date(job.created_at).toLocaleString()}
                  </td>
                  <td>
                    {job.result ? (
                      <a
                        href={`/api/download/${job.id}`}
                        download
                        className="text-xs font-medium text-cyan-400 hover:text-cyan-300 transition-colors inline-flex items-center gap-1"
                      >
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                          <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                          <polyline points="7 10 12 15 17 10" />
                          <line x1="12" y1="15" x2="12" y2="3" />
                        </svg>
                        Download
                      </a>
                    ) : (
                      <span className="text-slate-600">—</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
