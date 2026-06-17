"use client";

import React, { useState, useEffect } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { Sparkles, Loader2 } from "lucide-react";
import bcrypt from "bcryptjs";

export default function SigninPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState(false);

  // If already authenticated, redirect straight to dashboard
  useEffect(() => {
    const token = localStorage.getItem("token");
    if (token) {
      router.push("/dashboard");
    }
  }, [router]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email || !password) return;
    setError("");
    setLoading(true);

    try {
      // Simulate client-side network latency slightly
      await new Promise(resolve => setTimeout(resolve, 500));

      const normalizedEmail = email.toLowerCase().trim();
      const usersRaw = localStorage.getItem("ddc_users");
      const users = usersRaw ? JSON.parse(usersRaw) : [];

      // Auto-provision default developer account if empty
      if (users.length === 0 && normalizedEmail === "developer@copilot.local") {
        const salt = bcrypt.genSaltSync(10);
        const passwordHash = bcrypt.hashSync("password123", salt);
        const devUser = {
          id: "usr_developer",
          name: "Developer",
          email: "developer@copilot.local",
          passwordHash: passwordHash
        };
        users.push(devUser);
        localStorage.setItem("ddc_users", JSON.stringify(users));
      }

      const user = users.find((u: any) => u.email === normalizedEmail);
      if (!user) {
        throw new Error("Invalid email or password combination.");
      }

      const isPasswordValid = bcrypt.compareSync(password, user.passwordHash);
      if (!isPasswordValid) {
        throw new Error("Invalid email or password combination.");
      }

      // Store a simple secure mock token on successful authentication
      const mockToken = `mock_jwt_token_${user.id}_${Date.now()}`;
      localStorage.setItem("token", mockToken);

      setSuccess(true);
      setTimeout(() => {
        router.push("/dashboard");
      }, 1000);
    } catch (err: any) {
      setError(err.message || "An unexpected error occurred.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen w-screen flex flex-col dia-mesh-bg text-text-primary overflow-x-hidden relative select-none">
      
      {/* Top Navigation Bar */}
      <nav className="h-16 w-full bg-white/60 backdrop-blur-md border-b border-border-custom flex items-center justify-between px-8 sticky top-0 z-30">
        <div className="flex items-center gap-2.5">
          <div className="flex items-center justify-center w-7 h-7 rounded-lg bg-black text-white shadow-sm shrink-0">
            <Sparkles className="w-4.5 h-4.5 text-primary" />
          </div>
          <span className="font-display font-bold text-sm md:text-base tracking-tight text-text-primary">
            Databricks <span className="text-primary font-bold">Developer Copilot</span>
          </span>
        </div>
      </nav>

      {/* Centered Signin Container */}
      <main className="flex-1 flex items-center justify-center px-6 py-12">
        <div className="max-w-md w-full bg-white rounded-3xl border border-stone-200/90 shadow-md p-8 flex flex-col gap-6 text-left relative z-10">
          
          <div className="space-y-1 text-center">
            <h2 className="text-2xl font-bold tracking-tight text-text-primary">
              Welcome back
            </h2>
            <p className="text-xs text-text-secondary leading-relaxed">
              Sign in to access your Databricks Developer Copilot
            </p>
          </div>

          {error && (
            <div className="p-3 bg-red-50 border border-red-200 text-red-800 text-xs rounded-xl font-mono">
              {error}
            </div>
          )}

          {success ? (
            <div className="p-4 bg-green-50 border border-green-200 rounded-xl text-center text-xs text-green-800 font-mono flex flex-col items-center gap-3">
              <span className="font-bold">Login successful!</span>
              <p className="leading-relaxed">Redirecting to your dashboard...</p>
            </div>
          ) : (
            <form onSubmit={handleSubmit} className="flex flex-col gap-4">
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
                disabled={loading}
                className="w-full bg-black text-white hover:bg-black/90 font-semibold py-2.5 rounded-xl text-xs shadow transition cursor-pointer mt-2 text-center flex items-center justify-center gap-2"
              >
                {loading && <Loader2 className="w-3.5 h-3.5 animate-spin" />}
                <span>Sign In</span>
              </button>
            </form>
          )}

          {/* Redirect */}
          <div className="text-center text-xs text-text-secondary font-sans mt-2">
            <span>Don't have an account? </span>
            <Link href="/signup" className="text-primary hover:underline font-semibold">
              Sign up
            </Link>
          </div>

        </div>
      </main>

    </div>
  );
}
