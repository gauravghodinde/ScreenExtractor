import { NextRequest, NextResponse } from 'next/server';

const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:8080';

export async function POST(request: NextRequest, { params }: { params: Promise<{ path: string[] }> }) {
  const { path } = await params;
  const pathname = '/' + path.join('/');

  try {
    const contentType = request.headers.get('content-type');
    let body: any;

    // Handle different content types
    if (contentType?.includes('application/json')) {
      body = await request.json();
    } else {
      body = await request.arrayBuffer();
    }

    const url = `${BACKEND_URL}${pathname}`;
    const fetchOptions: RequestInit = {
      method: 'POST',
      headers: {
        'Content-Type': contentType || 'application/json',
      },
    };

    if (body instanceof ArrayBuffer) {
      fetchOptions.body = body;
    } else if (contentType?.includes('application/json')) {
      fetchOptions.body = JSON.stringify(body);
    } else {
      fetchOptions.body = body;
    }

    const response = await fetch(url, fetchOptions);
    const responseBody = await response.text();

    return new NextResponse(responseBody, {
      status: response.status,
      headers: {
        'Content-Type': response.headers.get('content-type') || 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    });
  } catch (error) {
    console.error('API proxy error:', error);
    return NextResponse.json(
      { error: 'Failed to reach backend service' },
      { status: 503 }
    );
  }
}

export async function GET(request: NextRequest, { params }: { params: Promise<{ path: string[] }> }) {
  const { path } = await params;
  const pathname = '/' + path.join('/');
  const searchParams = request.nextUrl.search;

  try {
    const url = `${BACKEND_URL}${pathname}${searchParams}`;
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
      },
    });

    const responseBody = await response.text();
    return new NextResponse(responseBody, {
      status: response.status,
      headers: {
        'Content-Type': response.headers.get('content-type') || 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
    });
  } catch (error) {
    console.error('API proxy error:', error);
    return NextResponse.json(
      { error: 'Failed to reach backend service' },
      { status: 503 }
    );
  }
}
