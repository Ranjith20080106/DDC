"use client";

import React, { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import {
  Play,
  Copy,
  Download,
  Check,
  Sparkles,
  RefreshCw,
  Send,
  LogOut,
  Search,
  HelpCircle
} from "lucide-react";
import { GoogleGenerativeAI } from "@google/generative-ai";

// Read API key from env (prefixed with NEXT_PUBLIC_ for client access) with fallback
const API_KEY = process.env.NEXT_PUBLIC_GEMINI_API_KEY;
const genAI = new GoogleGenerativeAI(API_KEY);

interface Notebook {
  id: string;
  name: string;
  prompt: string;
  code: string;
}

const DEFAULT_NOTEBOOK: Notebook = {
  id: "process_customer_data.py",
  name: "process_customer_data.py",
  prompt: "Read customer data from ADLS, remove duplicates using latest timestamp, and load into a Delta table.",
  code: `from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import logging

# Set up logging for production traceability
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DatabricksDeveloperCopilot")

def process_customer_data(spark: SparkSession, source_path: str, target_table: str):
    logger.info(f"Reading raw customer data from ADLS: {source_path}")
    
    # 1. Read customer data from ADLS
    df_raw = spark.read \\
        .format("json") \\
        .option("header", "true") \\
        .option("inferSchema", "true") \\
        .load(source_path)
    
    logger.info("De-duplicating customer records by latest timestamp...")
    
    # 2. Window specification to group by customer_id and order by updated_at descending
    window_spec = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())
    
    # Remove duplicates keeping the latest record
    df_deduped = df_raw \\
        .withColumn("row_num", row_number().over(window_spec)) \\
        .filter(col("row_num") == 1) \\
        .drop("row_num")
    
    logger.info(f"Writing de-duplicated customer data to Delta Table: {target_table}")
    
    # 3. Write data to Delta Lake table
    df_deduped.write \\
        .format("delta") \\
        .mode("overwrite") \\
        .option("mergeSchema", "true") \\
        .saveAsTable(target_table)
        
    logger.info("Pipeline executed successfully!")

if __name__ == "__main__":
    # Initialize Databricks Spark Session
    spark = SparkSession.builder \\
        .appName("DDC_Customer_Pipeline") \\
        .getOrCreate()
        
    source_adls_path = "abfss://raw-data@datalakeprod.dfs.core.windows.net/customers/v1/"
    target_delta_table = "silver.customers_deduped"
    
    process_customer_data(spark, source_adls_path, target_delta_table)
`
};

function stripMarkdownFormatting(rawText: string): string {
  let clean = rawText.trim();
  if (clean.startsWith("```python")) {
    clean = clean.substring(9);
  } else if (clean.startsWith("```py")) {
    clean = clean.substring(5);
  } else if (clean.startsWith("```")) {
    clean = clean.substring(3);
  }
  
  if (clean.endsWith("```")) {
    clean = clean.substring(0, clean.length - 3);
  }
  return clean.trim();
}

export default function DashboardPage() {
  const router = useRouter();
  const [isAuthenticated, setIsAuthenticated] = useState<boolean | null>(null);
  const [prompt, setPrompt] = useState<string>(DEFAULT_NOTEBOOK.prompt);
  const [editorCode, setEditorCode] = useState<string>("");
  const [isGenerating, setIsGenerating] = useState<boolean>(false);
  const [generationLineIndex, setGenerationLineIndex] = useState<number>(0);
  
  // Suggestions state
  const [suggestions, setSuggestions] = useState([
    { id: "logging", text: "Logging", active: true },
    { id: "delta_opt", text: "Optimize Delta", active: false },
    { id: "cache", text: "Cache DF", active: false }
  ]);

  const [copied, setCopied] = useState<boolean>(false);
  const [workspaceUrl] = useState("https://adb-179347590123.14.azuredatabricks.net");

  const [notification, setNotification] = useState<{ show: boolean; text: string; type: "success" | "info" }>({
    show: false,
    text: "",
    type: "info"
  });

  // Check JWT token existence and redirect if not logged in
  useEffect(() => {
    const token = localStorage.getItem("token");
    if (!token) {
      router.push("/signin");
    } else {
      setIsAuthenticated(true);
      // Pre-fill editor with a default example
      setEditorCode(DEFAULT_NOTEBOOK.code);
      setGenerationLineIndex(DEFAULT_NOTEBOOK.code.split("\n").length);
    }
  }, [router]);

  // Show dynamic notification
  const triggerNotification = (text: string, type: "success" | "info" = "info") => {
    setNotification({ show: true, text, type });
    setTimeout(() => {
      setNotification(prev => ({ ...prev, show: false }));
    }, 3000);
  };

  // Run PySpark code generation using the Gemini API directly from the client browser
  const handleGenerate = async () => {
    if (isGenerating || !prompt.trim()) return;

    setIsGenerating(true);
    setGenerationLineIndex(0);
    triggerNotification("Initiating client-side PySpark generator...", "info");

    try {
      const token = localStorage.getItem("token");
      if (!token) {
        router.push("/signin");
        return;
      }

      // Try multiple models on the client
      const modelsToTry = [
        "gemini-1.5-flash",
        "gemini-1.5-pro",
        "gemini-2.5-flash",
        "gemini-2.0-flash",
        "gemini-pro"
      ];

      let generatedCode = "";
      let lastError = null;

      for (const modelName of modelsToTry) {
        try {
          const model = genAI.getGenerativeModel({
            model: modelName,
            generationConfig: {
              temperature: 0.2,
              topP: 0.95,
            },
          });

          const geminiPrompt = `You are a Databricks Principal Data Engineer. Generate a production-ready, clean, well-documented PySpark script or notebook code that meets the following requirement:
      
Requirement: "${prompt}"

Your generated code must strictly adhere to the following standards:
1. Include all necessary library imports.
2. Properly initialize/get a SparkSession.
3. Configure logging for traceability.
4. Define schema explicitly where data is read.
5. Follow PEP 8 styles and add inline comments.
6. Write out to Delta Lake tables and include optimized operations like mergeSchema, partitions, or ZORDER if applicable.
7. Do NOT include Azure Databricks-specific connection settings, mount setups, or credential configurations (such as service principals, OAuth, SAS keys, or dbutils.secrets). Focus strictly on core PySpark data transformations and storage operations.

Return ONLY executable Python/PySpark code. Do NOT add preamble or conversational text.`;

          const result = await model.generateContent(geminiPrompt);
          const response = await result.response;
          const text = response.text();
          if (text) {
            generatedCode = stripMarkdownFormatting(text);
            break;
          }
        } catch (err: any) {
          console.warn(`Client model call ${modelName} failed:`, err);
          lastError = err;
        }
      }

      if (!generatedCode) {
        throw new Error(`All Gemini models failed. Last error: ${lastError?.message || "Unknown error"}`);
      }

      // Line-by-line streaming animation
      setEditorCode(generatedCode);
      const lines = generatedCode.split("\n");
      let currentLine = 0;

      const interval = setInterval(() => {
        currentLine += Math.min(3, lines.length - currentLine);
        setGenerationLineIndex(currentLine);

        if (currentLine >= lines.length) {
          clearInterval(interval);
          setIsGenerating(false);
          triggerNotification("PySpark script ready!", "success");
        }
      }, 70);

    } catch (error: any) {
      console.error("Code generation error:", error);
      setEditorCode(`# Error generating code:\n# ${error.message || "Unknown error occurred"}`);
      setGenerationLineIndex(2);
      setIsGenerating(false);
      triggerNotification("Generation failed", "info");
    }
  };

  const handleLogout = () => {
    localStorage.removeItem("token");
    router.push("/signin");
  };

  const toggleSuggestion = (id: string) => {
    setSuggestions(prev =>
      prev.map(s => (s.id === id ? { ...s, active: !s.active } : s))
    );
  };

  const handleCopyCode = () => {
    navigator.clipboard.writeText(editorCode);
    setCopied(true);
    triggerNotification("Copied code to clipboard", "success");
    setTimeout(() => setCopied(false), 2000);
  };

  const handleDownloadCode = () => {
    const element = document.createElement("a");
    const file = new Blob([editorCode], { type: "text/plain" });
    element.href = URL.createObjectURL(file);
    element.download = "pyspark_job.py";
    document.body.appendChild(element);
    element.click();
    document.body.removeChild(element);
    triggerNotification("Script file downloaded", "success");
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleGenerate();
    }
  };

  // Custom Python Syntax Highlighter
  const renderHighlightedCode = () => {
    const rawLines = editorCode.split("\n");
    const activeLines = rawLines.slice(0, generationLineIndex);

    return activeLines.map((line, idx) => {
      if (line.trim() === "") {
        return (
          <div key={idx} className="flex min-h-[1.5rem] select-none hover:bg-stone-50 px-4 font-mono text-xs md:text-sm">
            <span className="w-10 text-right pr-4 text-stone-400 select-none border-r border-stone-200">{idx + 1}</span>
            <span className="pl-4">&nbsp;</span>
          </div>
        );
      }

      let highlighted = line
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");

      const tokens: { placeholder: string; html: string }[] = [];
      let tokenCounter = 0;
      const tokenRegex = /#.*|"[^"\\]*(?:\\.[^"\\]*)*"|'[^'\\]*(?:\\.[^'\\]*)*'/g;

      highlighted = highlighted.replace(tokenRegex, (match) => {
        const placeholder = `___TOKEN_PLACEHOLDER_${tokenCounter}___`;
        tokenCounter++;
        let html = "";
        if (match.startsWith("#")) {
          html = `<span class="text-stone-400 italic">${match}</span>`;
        } else {
          html = `<span class="text-teal-700 font-medium">${match}</span>`;
        }
        tokens.push({ placeholder, html });
        return placeholder;
      });

      const keywords = [
        "def", "class", "import", "from", "as", "return", "if", "else", "elif",
        "try", "except", "finally", "while", "for", "in", "and", "or", "not",
        "with", "pass", "True", "False", "None", "print", "main"
      ];
      keywords.forEach(kw => {
        const regex = new RegExp(`\\b${kw}\\b`, "g");
        highlighted = highlighted.replace(regex, `<span class="text-primary font-semibold">${kw}</span>`);
      });

      const sparkKeywords = [
        "SparkSession", "Window", "col", "row_number", "desc", "partitionBy", "orderBy", "spark", "logger"
      ];
      sparkKeywords.forEach(skw => {
        const regex = new RegExp(`\\b${skw}\\b`, "g");
        highlighted = highlighted.replace(regex, `<span class="text-secondary font-medium">${skw}</span>`);
      });

      const sparkOperations = [
        "read", "write", "format", "option", "load", "mode", "filter", "select",
        "withColumn", "drop", "saveAsTable", "over", "builder", "appName",
        "getOrCreate", "basicConfig", "getLogger", "info", "error"
      ];
      sparkOperations.forEach(so => {
        const regex = new RegExp(`\\b${so}\\b`, "g");
        highlighted = highlighted.replace(regex, `<span class="text-stone-800 font-bold">${so}</span>`);
      });

      tokens.forEach(({ placeholder, html }) => {
        highlighted = highlighted.replace(placeholder, html);
      });

      return (
        <div key={idx} className="flex hover:bg-stone-50 px-4 font-mono text-xs md:text-sm">
          <span className="w-10 text-right pr-4 text-stone-400 select-none border-r border-stone-200">{idx + 1}</span>
          <span className="pl-4 flex-1 whitespace-pre text-stone-800" dangerouslySetInnerHTML={{ __html: highlighted }} />
        </div>
      );
    });
  };

  if (isAuthenticated === null) {
    return (
      <div className="min-h-screen w-screen flex items-center justify-center bg-stone-50">
        <div className="flex flex-col items-center gap-2">
          <Sparkles className="w-8 h-8 text-primary animate-pulse" />
          <span className="text-xs font-mono text-text-secondary">Authenticating...</span>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen w-screen flex flex-col dia-mesh-bg text-text-primary overflow-x-hidden relative pb-24 select-none animate-fade-in">
      
      {/* Top Navigation Bar */}
      <nav className="h-16 w-full bg-white/60 backdrop-blur-md border-b border-border-custom flex items-center justify-between px-8 sticky top-0 z-30 select-none">
        <div className="flex items-center gap-2.5">
          <div className="flex items-center justify-center w-7 h-7 rounded-lg bg-black text-white shadow-sm shrink-0">
            <Sparkles className="w-4.5 h-4.5 text-primary" />
          </div>
          <span className="font-display font-bold text-sm md:text-base tracking-tight text-text-primary">
            Databricks <span className="text-primary font-bold">Developer Copilot</span>
          </span>
        </div>

        {/* Logout Button */}
        <button 
          onClick={handleLogout}
          className="text-xs font-semibold text-text-secondary hover:text-red-600 px-3 py-1.5 rounded-full hover:bg-red-50 transition flex items-center gap-1.5 border border-stone-200/80 bg-white shadow-sm cursor-pointer"
        >
          <LogOut className="w-3.5 h-3.5" />
          <span>Sign Out</span>
        </button>
      </nav>

      {/* Floating Status Notification */}
      {notification.show && (
        <div className="fixed top-20 right-8 z-50 animate-slide-in">
          <div className={`px-4 py-2.5 rounded-2xl shadow-md border text-xs font-mono flex items-center gap-2 bg-stone-900 border-stone-800 text-stone-100`}>
            <div className="w-1.5 h-1.5 rounded-full bg-primary animate-ping" />
            {notification.text}
          </div>
        </div>
      )}

      {/* Centered Header & Branding */}
      <div className="max-w-4xl w-full mx-auto px-6 pt-12 text-center flex flex-col items-center gap-6">

        {/* Logo (Centered) */}
        <div className="flex items-center gap-2 px-3 py-1 bg-white/80 border border-border-custom rounded-full shadow-sm">
          <div className="w-4.5 h-4.5 rounded-full bg-primary flex items-center justify-center font-bold text-[9px] text-white">
            D
          </div>
          <span className="font-semibold text-[11px] tracking-tight text-text-primary uppercase font-mono">
            Copilot
          </span>
        </div>

        {/* Title (Centered, large) */}
        <h1 className="text-4xl md:text-5xl font-extrabold tracking-tight text-text-primary max-w-2xl leading-none">
          Chat with your lakehouse
        </h1>

        {/* Subtitle / Cluster Status Pill */}
        <div className="flex flex-col sm:flex-row items-center gap-3 bg-[#EAE8E2]/50 border border-border-custom p-1 px-3.5 rounded-full text-xs font-mono text-text-secondary select-none">
          <div className="flex items-center gap-1.5">
            <span className="w-2 h-2 rounded-full bg-success animate-pulse" />
            <span>standard_d8s_v3 (active)</span>
          </div>
          <span className="hidden sm:inline opacity-40">|</span>
          <span className="truncate max-w-[200px]">{workspaceUrl}</span>
        </div>

      </div>

      {/* Main Workspace Frame */}
      <main className="max-w-3xl w-full mx-auto px-6 pt-10 flex flex-col gap-8 text-center items-center">

        {/* Floating Capsule Chat Input */}
        <div className="w-full bg-white rounded-3xl border border-stone-200/90 shadow-md p-4 relative flex flex-col gap-3 focus-within:border-stone-400 focus-within:shadow-lg transition-all duration-300 z-10 text-left">

          <div className="flex gap-3 items-start">
            <Search className="w-5 h-5 text-text-secondary shrink-0 mt-2.5" />
            <textarea
              value={prompt}
              onChange={(e) => setPrompt(e.target.value)}
              onKeyDown={handleKeyDown}
              rows={1}
              className="w-full min-h-[44px] bg-transparent text-lg border-0 focus:ring-0 outline-none resize-none leading-relaxed p-1.5 text-text-primary placeholder-stone-400 font-sans"
              placeholder="Hey Copilot... (Press Enter to generate PySpark code)"
              disabled={isGenerating}
            />
          </div>

          {/* Action pill checkboxes + submit button */}
          <div className="border-t border-stone-100 pt-3 flex items-center justify-between">
            <div className="flex flex-wrap gap-2.5">
              {suggestions.map((item) => (
                <button
                  key={item.id}
                  onClick={() => toggleSuggestion(item.id)}
                  className={`px-3 py-1 rounded-full border text-xs font-medium transition duration-150 ${item.active
                      ? "bg-stone-900 border-stone-950 text-white"
                      : "bg-stone-50 border-stone-200 text-text-secondary hover:bg-stone-100"
                    }`}
                >
                  + {item.text}
                </button>
              ))}
            </div>

            {/* Black Circle Submit button */}
            <button
              onClick={handleGenerate}
              disabled={isGenerating || !prompt.trim()}
              className={`w-8 h-8 rounded-full flex items-center justify-center text-white transition shadow-sm ${prompt.trim() && !isGenerating
                  ? "bg-black hover:bg-black/90 cursor-pointer"
                  : "bg-stone-200 text-stone-400 cursor-not-allowed"
                }`}
            >
              {isGenerating ? (
                <RefreshCw className="w-3.5 h-3.5 animate-spin" />
              ) : (
                <Send className="w-3.5 h-3.5" />
              )}
            </button>
          </div>

        </div>

        {/* Faint browser mockups in background */}
        <div className="w-full flex flex-col bg-white border border-border-custom rounded-2xl shadow-sm overflow-hidden text-left transition-all duration-300">

          {/* macOS Browser Header */}
          <div className="px-4 py-2.5 bg-stone-50 border-b border-border-custom flex items-center justify-between select-none">
            {/* 3 colored dots */}
            <div className="flex items-center gap-1.5">
              <span className="w-3 h-3 rounded-full bg-red-400" />
              <span className="w-3 h-3 rounded-full bg-yellow-400" />
              <span className="w-3 h-3 rounded-full bg-green-400" />
            </div>

            {/* URL/File tab bar */}
            <div className="bg-white border border-border-custom rounded-md px-12 py-1 text-[11px] font-mono text-text-secondary">
              process_customer_data.py
            </div>

            {/* Actions */}
            <div className="flex items-center gap-1.5 text-text-secondary">
              <button
                onClick={handleCopyCode}
                className="p-1 rounded hover:bg-stone-200 hover:text-text-primary transition"
                title="Copy Code"
              >
                {copied ? <Check className="w-3.5 h-3.5 text-success" /> : <Copy className="w-3.5 h-3.5" />}
              </button>
              <button
                onClick={handleDownloadCode}
                className="p-1 rounded hover:bg-stone-200 hover:text-text-primary transition"
                title="Download Script"
              >
                <Download className="w-3.5 h-3.5" />
              </button>
            </div>
          </div>

          {/* Editor contents */}
          <div className="bg-white py-4 overflow-y-auto min-h-[320px] max-h-[480px] flex flex-col select-text font-mono border-b border-border-custom relative">
            {renderHighlightedCode()}

            {isGenerating && (
              <div className="px-4 flex items-center py-1 font-mono text-[10px] text-text-secondary/60 animate-pulse">
                <span className="w-10 text-right pr-4 border-r border-slate-200 select-none">···</span>
                <span className="pl-4 italic flex items-center gap-1">
                  <RefreshCw className="w-3 h-3 animate-spin text-primary" /> Synthesizing PySpark job...
                </span>
              </div>
            )}
          </div>

          {/* Footer status info */}
          <div className="px-4 py-2 bg-stone-50 text-[10px] font-mono text-text-secondary flex justify-between">
            <span>Lines: {editorCode.split("\n").length} • Python 3.10</span>
            <span>Spark 3.4.1</span>
          </div>

        </div>

      </main>

    </div>
  );
}
