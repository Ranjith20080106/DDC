"use client";

import React, { useState } from "react";
import Link from "next/link";
import { ArrowRight, Sparkles } from "lucide-react";

export default function SignupPage() {
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [submitted, setSubmitted] = useState(false);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!name || !email || !password) return;
    setSubmitted(true);
  };

  return (
    <div className="min-h-screen w-screen flex flex-col dia-mesh-bg text-text-primary overflow-x-hidden relative select-none">
      
      {/* Top Navigation Bar */}
      <nav className="h-16 w-full bg-white/60 backdrop-blur-md border-b border-border-custom flex items-center justify-between px-8 sticky top-0 z-30 select-none">
        {/* Left Side: Brand Name */}
        <Link href="/" className="flex items-center gap-2.5 hover:opacity-90 transition">
          <div className="flex items-center justify-center w-7 h-7 rounded-lg bg-black text-white shadow-sm shrink-0">
            <Sparkles className="w-4.5 h-4.5 text-primary" />
          </div>
          <span className="font-display font-bold text-sm md:text-base tracking-tight text-text-primary">
            Databricks <span className="text-primary font-bold">Developer Copilot</span>
          </span>
        </Link>

        {/* Right Side: Back to Workspace */}
        <Link 
          href="/"
          className="text-xs font-semibold text-text-secondary hover:text-text-primary px-3 py-1.5 transition flex items-center gap-1"
        >
          <span>Back to Workspace</span>
          <ArrowRight className="w-3.5 h-3.5" />
        </Link>
      </nav>

      {/* Centered Sign Up Box */}
      <main className="flex-1 flex items-center justify-center px-6 py-12">
        <div className="max-w-md w-full bg-white rounded-3xl border border-stone-200/90 shadow-md p-8 flex flex-col gap-6 text-left relative z-10">
          
          <div className="space-y-1 text-center">
            <h2 className="text-2xl font-bold tracking-tight text-text-primary">
              Create your account
            </h2>
            <p className="text-xs text-text-secondary leading-relaxed">
              Start generating production-ready PySpark code instantly
            </p>
          </div>

          {submitted ? (
            <div className="p-4 bg-green-50 border border-green-200 rounded-xl text-center text-xs text-green-800 font-mono flex flex-col items-center gap-3">
              <span className="font-bold">Registration successful!</span>
              <p className="leading-relaxed">Your account has been registered. Welcome to Developer Copilot!</p>
              <Link 
                href="/" 
                className="px-4 py-1.5 bg-black text-white text-xs font-semibold rounded-full hover:bg-black/90 transition text-center"
              >
                Go to Workspace
              </Link>
            </div>
          ) : (
            <form onSubmit={handleSubmit} className="flex flex-col gap-4">
              <div>
                <label className="block text-[10px] font-bold text-text-secondary uppercase font-mono mb-1">Full Name</label>
                <input
                  type="text"
                  required
                  placeholder="John Doe"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  className="w-full bg-stone-50 border border-border-custom px-3 py-2.5 rounded-xl text-xs text-text-primary outline-none focus:border-stone-400 font-sans"
                />
              </div>

              <div>
                <label className="block text-[10px] font-bold text-text-secondary uppercase font-mono mb-1">Email Address</label>
                <input
                  type="email"
                  required
                  placeholder="name@company.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="w-full bg-stone-50 border border-border-custom px-3 py-2.5 rounded-xl text-xs text-text-primary outline-none focus:border-stone-400 font-sans"
                />
              </div>

              <div>
                <label className="block text-[10px] font-bold text-text-secondary uppercase font-mono mb-1">Password</label>
                <input
                  type="password"
                  required
                  placeholder="••••••••"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="w-full bg-stone-50 border border-border-custom px-3 py-2.5 rounded-xl text-xs text-text-primary outline-none focus:border-stone-400 font-sans"
                />
              </div>

              <button
                type="submit"
                className="w-full bg-black text-white hover:bg-black/90 font-semibold py-2.5 rounded-xl text-xs shadow transition cursor-pointer mt-2 text-center"
              >
                Create Account
              </button>
            </form>
          )}

          {/* Redirect */}
          <div className="text-center text-xs text-text-secondary font-sans mt-2">
            <span>Already have an account? </span>
            <Link href="/login" className="text-primary hover:underline font-semibold">
              Login
            </Link>
          </div>

        </div>
      </main>

    </div>
  );
}
