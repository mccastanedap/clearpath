export default function UploadPage() {
  return (
    <div className="min-h-screen bg-[#f0f7f8] flex items-center justify-center px-5">
      <div className="w-full max-w-md">

        {/* Logo */}
        <div className="flex items-center justify-center gap-3 mb-10">
          <div className="leading-tight text-center">
            <div className="text-2xl font-semibold">
              <span className="text-[#112b50]">Clear</span>
              <span className="text-[#ef9f38]">path</span>
            </div>
            <div className="text-[11px] font-bold tracking-[0.2em] text-[#64b8c0]">DATA</div>
          </div>
        </div>

        {/* Card */}
        <div className="bg-white rounded-2xl shadow-sm border border-neutral-200 px-8 py-16">
          <h1 className="text-2xl font-bold text-[#112b50] text-center">Upload your data</h1>
          <p className="mt-3 text-sm text-neutral-500 text-center leading-relaxed">
            Upload your weekly sales CSV and receive AI-powered insights by email within minutes.
          </p>

          <div className="mt-12 space-y-8">
            <div>
              <label className="block text-sm font-medium text-neutral-700 mb-2">Business name</label>
              <input
                type="text"
                placeholder="e.g. Juice Bar NYC"
                className="w-full rounded-xl border border-neutral-200 bg-white px-4 py-3 text-sm text-neutral-900 placeholder-neutral-400 outline-none focus:border-[#64b8c0] focus:ring-2 focus:ring-[#64b8c0]/20 transition"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-neutral-700 mb-2">Sales CSV</label>
              <input
                type="file"
                accept=".csv"
                className="w-full text-sm text-neutral-500 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-medium file:bg-[#112b50] file:text-white hover:file:bg-[#1a3a6b] transition"
              />
            </div>

            <button className="w-full rounded-xl bg-[#64b8c0] py-3.5 text-sm font-semibold text-white hover:opacity-90 transition">
              Upload and run pipeline
            </button>
          </div>

          <p className="mt-10 text-center text-xs text-neutral-400">
            Don&apos;t have access?{" "}
            <a href="https://clearpath-site-theta.vercel.app/#contact" className="text-[#64b8c0] hover:underline">
              Contact us
            </a>
          </p>
        </div>

        <p className="mt-6 text-center text-xs text-neutral-400">
          <a href="https://clearpath-site-theta.vercel.app" className="hover:text-[#112b50] transition">
            ← Back to Clearpath Data
          </a>
        </p>
      </div>
    </div>
  );
}