const API_BASE = '/api';

export interface SearchResult {
  id: string;
  text: string;
  meta: {
    job_id: string;
    start: number;
    end: number;
    index?: number;
  };
  distance?: number;
  score?: number;
}

export interface Job {
  id: string;
  status: string;
  result: string | null;
  created_at: string;
}

export interface ClipJobResponse {
  clip_job_id: string;
  clip_start: number;
  clip_end: number;
  source_job: string;
  query: string;
}

// Upload video file
export async function uploadVideo(file: File): Promise<{ id: string; s3_path: string }> {
  const response = await fetch(`${API_BASE}/upload`, {
    method: 'POST',
    headers: {
      'Content-Length': file.size.toString(),
      'X-Filename': file.name,
    },
    body: file,
  });

  const data = await response.json();

  if (!response.ok) {
    throw new Error(data.error || `Upload failed: ${response.statusText}`);
  }

  return data;
}

// Search transcripts for a job
export async function searchTranscripts(
  query: string,
  jobId: string,
  topK: number = 5
): Promise<SearchResult[]> {
  const response = await fetch(`${API_BASE}/search`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      query,
      job_id: jobId,
      top_k: topK,
    }),
  });

  const data = await response.json();

  if (!response.ok) {
    throw new Error(data.error || `Search failed: ${response.statusText}`);
  }

  return data.results || [];
}

// Create a clip from a job
export async function createClip(
  query: string,
  jobId: string,
  topK: number = 1,
  padding: number = 0
): Promise<ClipJobResponse> {
  const response = await fetch(`${API_BASE}/clip/search`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      query,
      job_id: jobId,
      top_k: topK,
      padding,
    }),
  });

  const data = await response.json();

  if (!response.ok) {
    throw new Error(data.error || `Clip creation failed: ${response.statusText}`);
  }

  return data;
}

// Get all jobs
export async function getJobs(limit: number = 20, offset: number = 0, status?: string): Promise<Job[]> {
  const params = new URLSearchParams();
  params.append('limit', limit.toString());
  params.append('offset', offset.toString());
  if (status) {
    params.append('status', status);
  }

  const response = await fetch(`${API_BASE}/jobs?${params.toString()}`, {
    method: 'GET',
  });

  const data = await response.json();

  if (!response.ok) {
    throw new Error(data.error || `Failed to fetch jobs: ${response.statusText}`);
  }

  return data.jobs || [];
}

// Get job result URL
export async function getJobResultUrl(jobId: string): Promise<string> {
  return `${API_BASE}/jobs/${jobId}/result`;
}

// Health check
export async function healthCheck(): Promise<boolean> {
  try {
    const response = await fetch(`${API_BASE}/healthz`, {
      method: 'GET',
    });
    return response.ok;
  } catch {
    return false;
  }
}
