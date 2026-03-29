import Link from 'next/link';

export default function HomePage() {
  return (
    <div className="relative">
      {/* ─── Hero Section ─── */}
      <section className="relative py-24 lg:py-36 overflow-hidden">
        <div className="max-w-7xl mx-auto px-6">
          <div className="max-w-3xl mx-auto text-center animate-fade-in-up">
            <div className="section-label mx-auto w-fit mb-6">
              <span className="inline-block w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
              AI-Powered Video Intelligence
            </div>

            <h1 className="text-5xl lg:text-7xl font-extrabold tracking-tight leading-[1.1] mb-6">
              Find Any{' '}
              <span className="gradient-text">Scene</span>
              <br />
              In Seconds
            </h1>

            <p className="text-lg lg:text-xl text-slate-400 leading-relaxed mb-10 max-w-2xl mx-auto">
              Upload your videos and search through dialogue semantically.
              SceneExtractor indexes every word and retrieves the exact frame
              you&apos;re looking for — no manual scrubbing required.
            </p>

            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <Link href="/dashboard" className="btn-gradient text-center text-base inline-flex items-center justify-center gap-2">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
                  <polyline points="17 8 12 3 7 8" />
                  <line x1="12" y1="3" x2="12" y2="15" />
                </svg>
                Get Started
              </Link>
              <a href="#features" className="btn-secondary text-center inline-flex items-center justify-center gap-2">
                Learn More
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <line x1="12" y1="5" x2="12" y2="19" />
                  <polyline points="19 12 12 19 5 12" />
                </svg>
              </a>
            </div>
          </div>

          {/* Hero visual — abstract video grid */}
          <div className="mt-20 max-w-4xl mx-auto animate-fade-in-up-delay-1">
            <div className="glass-card p-1.5 rounded-2xl">
              <div className="rounded-xl overflow-hidden bg-gradient-to-br from-slate-900 via-indigo-950/50 to-slate-900 p-8 lg:p-12">
                <div className="grid grid-cols-3 gap-3">
                  {/* Simulated search bar */}
                  <div className="col-span-3 flex items-center gap-3 bg-black/30 rounded-lg px-4 py-3 border border-white/5">
                    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#8b5cf6" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <circle cx="11" cy="11" r="8" />
                      <line x1="21" y1="21" x2="16.65" y2="16.65" />
                    </svg>
                    <span className="text-slate-500 text-sm">&quot;I need a million dollars&quot;</span>
                    <span className="ml-auto text-xs text-violet-400 font-semibold px-3 py-1 bg-violet-500/10 rounded-md border border-violet-500/20">Search</span>
                  </div>
                  {/* Result cards */}
                  {[
                    { time: '01:23:45', score: '0.94', text: '"I need a million dollars to save this company"' },
                    { time: '00:45:12', score: '0.87', text: '"We need at least a million to break even"' },
                    { time: '02:10:03', score: '0.82', text: '"A million bucks would change everything"' },
                  ].map((item, i) => (
                    <div key={i} className="bg-black/20 rounded-lg p-4 border border-white/5 hover:border-violet-500/20 transition-colors">
                      <div className="flex items-center justify-between mb-2">
                        <span className="text-xs font-mono text-cyan-400">{item.time}</span>
                        <span className="text-xs font-semibold text-emerald-400">{item.score}</span>
                      </div>
                      <p className="text-xs text-slate-400 leading-relaxed">{item.text}</p>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* ─── Features Section ─── */}
      <section id="features" className="py-24 relative">
        <div className="max-w-7xl mx-auto px-6">
          <div className="text-center mb-16 animate-fade-in-up">
            <div className="section-label mx-auto w-fit">Features</div>
            <h2 className="text-3xl lg:text-5xl font-bold mt-4">
              Everything You Need to{' '}
              <span className="gradient-text">Search Video</span>
            </h2>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            {[
              {
                icon: (
                  <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                    <circle cx="11" cy="11" r="8" />
                    <line x1="21" y1="21" x2="16.65" y2="16.65" />
                    <path d="M11 8a3 3 0 0 0 0 6" />
                  </svg>
                ),
                title: 'Semantic Search',
                desc: 'Search by meaning, not keywords. Our AI understands context so you can describe what you\'re looking for naturally.',
                color: 'from-violet-500 to-indigo-500',
              },
              {
                icon: (
                  <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                    <polygon points="23 7 16 12 23 17 23 7" />
                    <rect x="1" y="5" width="15" height="14" rx="2" ry="2" />
                  </svg>
                ),
                title: 'Frame-Perfect Clips',
                desc: 'Extract exact video clips from any scene. Get the precise moment with customizable padding for context.',
                color: 'from-cyan-500 to-blue-500',
              },
              {
                icon: (
                  <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M12 2L2 7l10 5 10-5-10-5z" />
                    <path d="M2 17l10 5 10-5" />
                    <path d="M2 12l10 5 10-5" />
                  </svg>
                ),
                title: 'Distributed Pipeline',
                desc: 'Built for scale with Go orchestration, Python AI workers, and S3-compatible storage. Process videos of any size.',
                color: 'from-emerald-500 to-teal-500',
              },
            ].map((feat, i) => (
              <div
                key={i}
                className={`feature-card animate-fade-in-up-delay-${i + 1}`}
              >
                <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${feat.color} flex items-center justify-center mb-5 text-white`}>
                  {feat.icon}
                </div>
                <h3 className="text-xl font-bold text-white mb-3">{feat.title}</h3>
                <p className="text-slate-400 leading-relaxed text-sm">{feat.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ─── How It Works Section ─── */}
      <section className="py-24 relative">
        <div className="max-w-5xl mx-auto px-6">
          <div className="text-center mb-16">
            <div className="section-label mx-auto w-fit">Workflow</div>
            <h2 className="text-3xl lg:text-5xl font-bold mt-4">
              How It <span className="gradient-text">Works</span>
            </h2>
          </div>

          <div className="space-y-6">
            {[
              {
                step: '1',
                title: 'Upload Your Video',
                desc: 'Drop any video file — our system automatically extracts subtitles and dialogue using FFmpeg.',
              },
              {
                step: '2',
                title: 'AI Indexing',
                desc: 'Sentence-Transformers generate vector embeddings for every line of dialogue, creating a searchable index.',
              },
              {
                step: '3',
                title: 'Search & Extract',
                desc: 'Type a natural language query and get back the exact scene. Create downloadable clips with one click.',
              },
            ].map((item, i) => (
              <div key={i} className="glass-card p-6 flex items-start gap-5">
                <div className="step-number">{item.step}</div>
                <div>
                  <h3 className="text-lg font-bold text-white mb-1">{item.title}</h3>
                  <p className="text-slate-400 text-sm leading-relaxed">{item.desc}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* ─── CTA Footer ─── */}
      <section className="py-24 relative">
        <div className="max-w-3xl mx-auto px-6 text-center">
          <div className="glass-card p-12 lg:p-16">
            <h2 className="text-3xl lg:text-4xl font-bold mb-4">
              Ready to <span className="gradient-text">Extract Scenes</span>?
            </h2>
            <p className="text-slate-400 mb-8 max-w-lg mx-auto">
              Upload your first video and experience the power of AI-driven video search.
            </p>
            <Link href="/dashboard" className="btn-gradient text-base inline-flex items-center gap-2">
              Go to Dashboard
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <line x1="5" y1="12" x2="19" y2="12" />
                <polyline points="12 5 19 12 12 19" />
              </svg>
            </Link>
          </div>
        </div>
      </section>

      {/* ─── Footer ─── */}
      <footer className="border-t border-white/5 py-8">
        <div className="max-w-7xl mx-auto px-6 flex flex-col sm:flex-row items-center justify-between gap-4 text-sm text-slate-500">
          <span>© 2026 SceneExtractor. Built with ❤️</span>
          <div className="flex items-center gap-6">
            <a href="https://github.com" target="_blank" rel="noopener noreferrer" className="hover:text-white transition-colors">GitHub</a>
            <a href="#features" className="hover:text-white transition-colors">Features</a>
          </div>
        </div>
      </footer>
    </div>
  );
}
