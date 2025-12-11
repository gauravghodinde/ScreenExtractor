# Scene Extractor Frontend

A Next.js web application for uploading videos, searching transcripts, and creating clips with AI-powered search.

## Features

- **Video Upload**: Stream upload large video files directly to MinIO storage
- **Transcript Search**: Search through video transcripts using semantic or keyword search
- **Clip Creation**: Automatically extract clips based on search queries with adjustable padding
- **Job Management**: Monitor upload and processing jobs with real-time status updates
- **Job Results**: Download processed video clips

## Getting Started

### Prerequisites

- Node.js 18+ and npm
- Running Scene Extractor backend services (orchestrator at `http://localhost:8080`)

### Installation

```bash
cd frontend
npm install
```

### Configuration

Create or update `.env.local`:

```env
NEXT_PUBLIC_API_URL=http://localhost:8080
```

Adjust the API URL based on your backend deployment.

### Development

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser.

### Building for Production

```bash
npm run build
npm start
```

## API Endpoints

The frontend communicates with the following backend endpoints:

- `POST /upload` - Upload a video file
- `POST /search` - Search transcripts for a job
- `POST /clip/search` - Create a clip from a search query
- `GET /jobs` - List all jobs
- `GET /jobs/:id/result` - Download job result

## Components

- **UploadVideo**: Handle video file uploads
- **SearchTranscripts**: Search through transcripts with keyword/semantic search
- **CreateClip**: Generate clips based on search results
- **JobsList**: Display list of recent jobs with status and results

## Technologies

- **Next.js 15** - React framework with App Router
- **TypeScript** - Type-safe development
- **Tailwind CSS** - Utility-first CSS styling
- **React 19** - UI library

## Project Structure

```
src/
├── app/
│   ├── page.tsx          # Main home page
│   └── layout.tsx        # Root layout
├── components/
│   ├── UploadVideo.tsx    # Video upload component
│   ├── SearchTranscripts.tsx
│   ├── CreateClip.tsx
│   └── JobsList.tsx
└── lib/
    └── api.ts            # API client functions
```

## License

MIT
