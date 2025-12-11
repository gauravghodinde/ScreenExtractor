'use client';

import { useState } from 'react';
import UploadVideo from '@/components/UploadVideo';
import SearchTranscripts from '@/components/SearchTranscripts';
import CreateClip from '@/components/CreateClip';
import JobsList from '@/components/JobsList';

export default function Home() {
  const [currentJobId, setCurrentJobId] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);

  const handleUploadSuccess = (jobId: string) => {
    setCurrentJobId(jobId);
    setRefreshKey(k => k + 1);
  };

  return (
    <main className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100">
      <div className="max-w-6xl mx-auto px-4 py-8">
        {/* Header */}
        <div className="mb-8">
          <h1 className="text-4xl font-bold text-gray-900 mb-2">Scene Extractor</h1>
          <p className="text-lg text-gray-600">
            Upload videos, search transcripts, and create clips with AI-powered search
          </p>
        </div>

        {/* Main Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-8">
          {/* Upload Section */}
          <div className="lg:col-span-1">
            <UploadVideo onSuccess={handleUploadSuccess} />
          </div>

          {/* Jobs List */}
          <div className="lg:col-span-2">
            <JobsList key={refreshKey} />
          </div>
        </div>

        {/* Search and Clip Section */}
        {currentJobId && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
            <SearchTranscripts jobId={currentJobId} />
            <CreateClip jobId={currentJobId} />
          </div>
        )}

        {!currentJobId && (
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-8 text-center">
            <p className="text-gray-700">
              Upload a video or select a job from the list above to get started with search and clip creation.
            </p>
          </div>
        )}
      </div>
    </main>
  );
}
