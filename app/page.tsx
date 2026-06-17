"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import { Sparkles } from "lucide-react";

export default function RootPage() {
  const router = useRouter();

  useEffect(() => {
    const token = localStorage.getItem("token");
    if (token) {
      router.replace("/dashboard");
    } else {
      router.replace("/signup");
    }
  }, [router]);

  return (
    <div className="min-h-screen w-screen flex items-center justify-center bg-stone-50">
      <div className="flex flex-col items-center gap-2">
        <Sparkles className="w-8 h-8 text-primary animate-pulse" />
        <span className="text-xs font-mono text-text-secondary">Loading Databricks Developer Copilot...</span>
      </div>
    </div>
  );
}
