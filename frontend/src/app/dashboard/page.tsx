'use client';

import { useState } from 'react';
import UploadVideo from '@/components/UploadVideo';
import SearchTranscripts from '@/components/SearchTranscripts';
import CreateClip from '@/components/CreateClip';
import JobsList from '@/components/JobsList';

export default function DashboardPage() {
    const [currentJobId, setCurrentJobId] = useState<string | null>(null);
    const [refreshKey, setRefreshKey] = useState(0);

    const handleUploadSuccess = (jobId: string) => {
        setCurrentJobId(jobId);
        setRefreshKey(k => k + 1);
    };

    return (
        <div className="max-w-7xl mx-auto px-6 py-10">
            {/* Page Header */}
            <div className="mb-10 animate-fade-in-up">
                <h1 className="text-3xl lg:text-4xl font-bold mb-2">
                    <span className="gradient-text">Dashboard</span>
                </h1>
                <p className="text-slate-400 text-base">
                    Upload videos, track processing jobs, search transcripts, and create clips.
                </p>
            </div>

            {/* Upload + Jobs Grid */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8 animate-fade-in-up-delay-1">
                <div className="lg:col-span-1">
                    <UploadVideo onSuccess={handleUploadSuccess} />
                </div>
                <div className="lg:col-span-2">
                    <JobsList
                        key={refreshKey}
                        onSelectJob={(jobId) => setCurrentJobId(jobId)}
                    />
                </div>
            </div>

            {/* Search + Clip Section */}
            {currentJobId && (
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8 animate-fade-in-up">
                    <SearchTranscripts jobId={currentJobId} />
                    <CreateClip jobId={currentJobId} />
                </div>
            )}

            {!currentJobId && (
                <div className="glass-card p-10 text-center animate-fade-in-up-delay-2">
                    <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-indigo-500/20 to-violet-500/20 border border-indigo-500/20 flex items-center justify-center mx-auto mb-5">
                        <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="#8b5cf6" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                            <polygon points="23 7 16 12 23 17 23 7" />
                            <rect x="1" y="5" width="15" height="14" rx="2" ry="2" />
                        </svg>
                    </div>
                    <p className="text-slate-400 text-sm max-w-md mx-auto">
                        Upload a video or select a job from the list above to start searching transcripts and creating clips.
                    </p>
                </div>
            )}
        </div>
    );
}
