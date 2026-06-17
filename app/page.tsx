"use client";

import React, { useState, useEffect } from "react";
import Link from "next/link";
import {
  Play,
  Copy,
  Download,
  Check,
  FileCode,
  Layers,
  Sparkles,
  RefreshCw,
  Search,
  BookOpen,
  Settings,
  X,
  ArrowRight,
  Send,
  HelpCircle,
  Database
} from "lucide-react";

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

export default function DatabricksDeveloperCopilot() {
  const [prompt, setPrompt] = useState<string>(DEFAULT_NOTEBOOK.prompt);
  const [editorCode, setEditorCode] = useState<string>(DEFAULT_NOTEBOOK.code);
  const [isGenerating, setIsGenerating] = useState<boolean>(false);
  const [generationLineIndex, setGenerationLineIndex] = useState<number>(DEFAULT_NOTEBOOK.code.split("\n").length);
  const [activePipelineNode, setActivePipelineNode] = useState<number>(3); // 0, 1, 2, 3

  // Suggestions state
  const [suggestions, setSuggestions] = useState([
    { id: "logging", text: "Logging", active: true },
    { id: "delta_opt", text: "Optimize Delta", active: false },
    { id: "cache", text: "Cache DF", active: false }
  ]);

  const [copied, setCopied] = useState<boolean>(false);
  const [activeOverlay, setActiveOverlay] = useState<string | null>(null); // "Docs" | "Settings" | "Pipeline" | null
  const [showOutput, setShowOutput] = useState<boolean>(false);
  const [submittedPrompt, setSubmittedPrompt] = useState<string>("");
  
  const [notification, setNotification] = useState<{ show: boolean; text: string; type: "success" | "info" }>({
    show: false,
    text: "",
    type: "info"
  });

  // Settings
  const [workspaceUrl, setWorkspaceUrl] = useState("https://adb-179347590123.14.azuredatabricks.net");
  const [defaultCatalog, setDefaultCatalog] = useState("main");
  const [defaultSchema, setDefaultSchema] = useState("silver");

  // Show dynamic notification
  const triggerNotification = (text: string, type: "success" | "info" = "info") => {
    setNotification({ show: true, text, type });
    setTimeout(() => {
      setNotification(prev => ({ ...prev, show: false }));
    }, 3000);
  };

  // Run PySpark code generation stream simulation
  const handleGenerate = () => {
    if (isGenerating) return;

    setShowOutput(true);
    setSubmittedPrompt(prompt);
    setIsGenerating(true);
    setGenerationLineIndex(0);
    setActivePipelineNode(0);
    triggerNotification("Assembling ingestion flow...", "info");

    const fullCode = DEFAULT_NOTEBOOK.code;
    let customizedCode = fullCode;

    const deltaActive = suggestions.find(s => s.id === "delta_opt")?.active;
    const cacheActive = suggestions.find(s => s.id === "cache")?.active;
    const loggingActive = suggestions.find(s => s.id === "logging")?.active;

    if (!loggingActive) {
      customizedCode = customizedCode
        .replace("import logging", "")
        .replace(/logger\.\w+\(.*?\)/g, "# logging disabled");
    }

    if (cacheActive) {
      customizedCode = customizedCode.replace(
        "df_deduped = df_raw",
        "df_deduped = df_raw.cache() # Cache raw data for reuse"
      );
    }

    if (deltaActive) {
      customizedCode = customizedCode.replace(
        "logger.info(\"Pipeline executed successfully!\")",
        `# Z-Order Optimization\n    logger.info("Optimizing Delta table layouts...")\n    spark.sql(f"OPTIMIZE {target_table} ZORDER BY (customer_id)")\n    logger.info("Pipeline executed successfully!")`
      );
    }

    const customLines = customizedCode.split("\n");
    let currentLine = 0;

    const interval = setInterval(() => {
      currentLine++;
      setGenerationLineIndex(currentLine);

      const progressRatio = currentLine / customLines.length;
      if (progressRatio < 0.25) {
        setActivePipelineNode(0);
      } else if (progressRatio < 0.5) {
        setActivePipelineNode(1);
      } else if (progressRatio < 0.75) {
        setActivePipelineNode(2);
      } else {
        setActivePipelineNode(3);
      }

      if (currentLine >= customLines.length) {
        clearInterval(interval);
        setIsGenerating(false);
        setEditorCode(customizedCode);
        triggerNotification("Compilation finished", "success");
      }
    }, 35);
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
    element.download = DEFAULT_NOTEBOOK.id;
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

  // Custom Python Syntax Highlighter adapted for warm layout
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

      const commentRegex = /(#.*)$/g;
      highlighted = highlighted.replace(commentRegex, '<span class="text-stone-400 italic">$1</span>');
      highlighted = highlighted.replace(/(".*?")/g, '<span class="text-teal-700 font-medium">$1</span>');
      highlighted = highlighted.replace(/('.*?')/g, '<span class="text-teal-700 font-medium">$1</span>');

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

      return (
        <div key={idx} className="flex hover:bg-stone-50 px-4 font-mono text-xs md:text-sm">
          <span className="w-10 text-right pr-4 text-stone-400 select-none border-r border-stone-200">{idx + 1}</span>
          <span className="pl-4 flex-1 whitespace-pre-wrap break-words text-stone-800" dangerouslySetInnerHTML={{ __html: highlighted }} />
        </div>
      );
    });
  };

  return (
    <div className="min-h-screen w-screen flex flex-col dia-mesh-bg text-text-primary overflow-x-hidden relative pb-24 select-none">
      
      {/* Top Navigation Bar */}
      <nav className="h-16 w-full bg-white/60 backdrop-blur-md border-b border-border-custom flex items-center justify-between px-8 sticky top-0 z-30 select-none">
        {/* Left Side: Brand Name */}
        <div className="flex items-center gap-2.5">
          <div className="flex items-center justify-center w-7 h-7 rounded-lg bg-black text-white shadow-sm shrink-0">
            <Sparkles className="w-4.5 h-4.5 text-primary" />
          </div>
          <span className="font-display font-bold text-sm md:text-base tracking-tight text-text-primary select-none">
            Databricks <span className="text-primary font-bold">Developer Copilot</span>
          </span>
        </div>

        {/* Right Side: Sign Up & Login Buttons */}
        <div className="flex items-center gap-4">
          <Link 
            href="/login"
            className="text-xs font-semibold text-text-secondary hover:text-text-primary px-3 py-1.5 transition"
          >
            Login
          </Link>
          <Link 
            href="/signup"
            className="bg-black text-white hover:bg-black/90 text-xs font-semibold px-4 py-1.5 rounded-full shadow-sm transition"
          >
            Sign Up
          </Link>
        </div>
      </nav>

      {/* Centered Header & Branding */}
      <div className="max-w-4xl w-full mx-auto px-6 pt-12 text-center flex flex-col items-center gap-6">

        {/* Title (Centered, large) */}
        <h1 className="text-4xl md:text-5xl font-extrabold tracking-tight text-text-primary max-w-2xl leading-none">
          Write PySpark code at the speed of thought
        </h1>



      </div>

      {/* Main Workspace Frame */}
      <main className="max-w-4xl w-full mx-auto px-6 pt-10 flex flex-col gap-8 text-center items-center">
        
        {/* Floating Capsule Chat Input */}
        <div className="w-full max-w-xl bg-white rounded-full border border-stone-200 shadow-sm px-5 py-2 flex items-center gap-3 focus-within:border-stone-400 focus-within:shadow-md transition-all duration-300 z-10 text-left">
          <textarea
            value={prompt}
            onChange={(e) => setPrompt(e.target.value)}
            onKeyDown={handleKeyDown}
            rows={1}
            className="flex-1 bg-transparent text-xs md:text-sm border-0 focus:ring-0 outline-none resize-none leading-relaxed py-1.5 text-text-primary placeholder-stone-400"
            placeholder="Hey Copilot... (Press Enter to generate PySpark code)"
          />

          {/* Black Circle Submit button */}
          <button
            onClick={handleGenerate}
            disabled={isGenerating || !prompt.trim()}
            className={`w-8 h-8 rounded-full flex items-center justify-center text-white transition shadow-sm shrink-0 ${
              prompt.trim() && !isGenerating
                ? "bg-black hover:bg-black/90 cursor-pointer"
                : "bg-stone-200 text-stone-400 cursor-not-allowed"
            }`}
          >
            <Send className="w-3.5 h-3.5" />
          </button>
        </div>

        {/* Chat Conversation Thread (ChatGPT Style) */}
        {showOutput && (
          <div className="w-full max-w-4xl flex flex-col gap-8 text-left mt-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
            
            {/* User Message */}
            <div className="flex gap-4 items-start px-2">
              <div className="w-7 h-7 rounded-full bg-stone-200/80 text-stone-700 flex items-center justify-center font-bold text-xs shrink-0 select-none">
                U
              </div>
              <div className="flex-1 min-w-0">
                <div className="text-[10px] font-bold text-stone-400 uppercase tracking-wider mb-0.5 select-none">You</div>
                <p className="text-sm text-text-primary leading-relaxed whitespace-pre-wrap font-sans">
                  {submittedPrompt}
                </p>
              </div>
            </div>

            {/* Divider line between turns */}
            <div className="border-t border-stone-200/60 w-full" />

            {/* Assistant/Copilot Message */}
            <div className="flex gap-4 items-start px-2">
              <div className="w-7 h-7 rounded-full bg-black text-white flex items-center justify-center shrink-0 shadow-sm select-none">
                <Sparkles className="w-4 h-4 text-primary animate-pulse" />
              </div>
              <div className="flex-1 min-w-0 space-y-4">
                <div>
                  <div className="text-[10px] font-bold text-stone-400 uppercase tracking-wider mb-0.5 select-none">Copilot</div>
                  <p className="text-sm text-text-primary leading-relaxed font-sans">
                    Here is the optimized PySpark script matching your query. It reads data from the storage layer, handles de-duplication with a window function, and writes into Delta Lake:
                  </p>
                </div>

                {/* Minimal Notion Code block */}
                <div className="w-full flex flex-col bg-[#FDFDFB] border border-stone-200 rounded-2xl shadow-sm overflow-hidden transition-all duration-300">
                  {/* Minimal Header */}
                  <div className="px-5 py-3 bg-[#FAF9F5] border-b border-stone-200/60 flex items-center justify-between select-none">
                    {/* File Info */}
                    <div className="flex items-center gap-2 text-xs font-mono text-stone-600">
                      <FileCode className="w-4 h-4 text-stone-400" />
                      <span>process_customer_data.py</span>
                    </div>

                    {/* Actions */}
                    <div className="flex items-center gap-3">
                      <button
                        onClick={handleCopyCode}
                        className="flex items-center gap-1.5 text-xs text-stone-500 hover:text-stone-800 transition cursor-pointer"
                        title="Copy Code"
                      >
                        {copied ? <Check className="w-3.5 h-3.5 text-emerald-600" /> : <Copy className="w-3.5 h-3.5" />}
                        <span>{copied ? "Copied" : "Copy"}</span>
                      </button>
                      <button
                        onClick={handleDownloadCode}
                        className="flex items-center gap-1.5 text-xs text-stone-500 hover:text-stone-800 transition cursor-pointer"
                        title="Download Script"
                      >
                        <Download className="w-3.5 h-3.5" />
                        <span>Download</span>
                      </button>
                    </div>
                  </div>

                  {/* Editor contents */}
                  <div className="bg-[#FDFDFB] py-4 overflow-y-auto min-h-[320px] max-h-[480px] flex flex-col select-text font-mono rounded-b-2xl">
                    {renderHighlightedCode()}

                    {isGenerating && (
                      <div className="px-4 flex items-center py-1 font-mono text-[10px] text-text-secondary/60 animate-pulse">
                        <span className="w-10 text-right pr-4 border-r border-stone-200 select-none">···</span>
                        <span className="pl-4 italic flex items-center gap-1">
                          <RefreshCw className="w-3 h-3 animate-spin text-primary" /> Compiling ingestion scripts...
                        </span>
                      </div>
                    )}
                  </div>
                </div>

                {/* Explanation section fading in after generation */}
                <div className={`transition-all duration-700 ease-out ${
                  isGenerating ? "opacity-0 translate-y-2 max-h-0 overflow-hidden" : "opacity-100 translate-y-0 max-h-[500px]"
                }`}>
                  <div className="text-xs text-text-secondary space-y-2.5 leading-relaxed bg-white border border-stone-200/60 rounded-2xl p-4 shadow-xs">
                    <p className="font-semibold text-text-primary flex items-center gap-1.5">
                      <Database className="w-3.5 h-3.5 text-primary" /> Ingestion & Processing Details:
                    </p>
                    <ul className="list-disc pl-4 space-y-1.5">
                      <li><strong className="text-text-primary">ADLS Source Ingestion:</strong> Connects to Azure Data Lake Storage Gen2 path using modern ABFSS driver endpoints.</li>
                      <li><strong className="text-text-primary">Deduplication Window:</strong> Filters duplicate entries on <code className="font-mono bg-stone-100 px-1 rounded text-[10px]">customer_id</code> by sorting descending timestamp logs via <code className="font-mono bg-stone-100 px-1 rounded text-[10px]">row_number()</code> ranking.</li>
                      <li><strong className="text-text-primary">Delta Target Lakehouse:</strong> Persists records into silver table targets utilizing schema merging to dynamically adapt schemas.</li>
                    </ul>
                  </div>
                </div>

              </div>
            </div>

          </div>
        )}

      </main>

      {/* 4. FLOATING BOTTOM NAVIGATION */}
      <div className="fixed bottom-6 left-1/2 -translate-x-1/2 bg-white/90 backdrop-blur-md rounded-full border border-stone-200 shadow-lg px-4 py-2 flex items-center gap-4 z-40 select-none">
        {[
          { id: "Docs", label: "API Reference", icon: BookOpen },
          { id: "Settings", label: "Settings", icon: Settings },
          { id: "Pipeline", label: "Pipeline Flow", icon: Layers }
        ].map((item) => {
          const Icon = item.icon;
          const isActive = activeOverlay === item.id;
          return (
            <button
              key={item.id}
              onClick={() => setActiveOverlay(isActive ? null : item.id)}
              className={`px-3 py-1 rounded-full text-xs font-semibold flex items-center gap-1.5 transition ${
                isActive
                  ? "bg-black text-white"
                  : "text-text-secondary hover:text-text-primary hover:bg-stone-100"
              }`}
            >
              <Icon className="w-3.5 h-3.5" />
              <span>{item.label}</span>
            </button>
          );
        })}
      </div>

      {/* OVERLAY MODAL MANAGER */}
      {activeOverlay && (
        <div className="fixed inset-0 bg-black/20 backdrop-blur-sm flex items-center justify-center p-6 z-50">
          <div className="bg-white rounded-3xl border border-stone-200 max-w-xl w-full shadow-2xl overflow-hidden flex flex-col relative animate-in fade-in zoom-in-95 duration-200 text-left">
            
            {/* Modal Header */}
            <div className="px-6 py-4 bg-stone-50 border-b border-border-custom flex items-center justify-between select-none">
              <span className="font-bold text-sm tracking-tight uppercase font-mono text-text-secondary">
                {activeOverlay === "Docs" && "API Reference"}
                {activeOverlay === "Settings" && "Workspace Settings"}
                {activeOverlay === "Pipeline" && "Pipeline Ingestion Flow"}
              </span>
              <button
                onClick={() => setActiveOverlay(null)}
                className="p-1 rounded-full hover:bg-stone-200 text-stone-500 hover:text-stone-700 transition"
              >
                <X className="w-4.5 h-4.5" />
              </button>
            </div>

            {/* Modal Body */}
            <div className="p-6 overflow-y-auto max-h-[70vh]">
              
              {activeOverlay === "Docs" && (
                <div className="space-y-4">
                  {[
                    {
                      title: "Window.partitionBy()",
                      syntax: "Window.partitionBy(*cols).orderBy(*cols)",
                      desc: "Groups data rows into partitions over which window aggregation is computed. Commonly used for running sums or ranking deduplications."
                    },
                    {
                      title: "df.write.format('delta')",
                      syntax: "df.write.format('delta').mode(saveMode).save(path)",
                      desc: "Saves the contents of the DataFrame as a Delta Lake table. Supports mergeSchema, overwriteSchema, and partitionBy options."
                    },
                    {
                      title: "OPTIMIZE table_name",
                      syntax: "spark.sql('OPTIMIZE table_name ZORDER BY (columns)')",
                      desc: "Coalesces small files in Delta tables to improve query speed. Z-Order clustering on high-cardinality filter columns optimizes read runtimes."
                    }
                  ].map((doc) => (
                    <div key={doc.title} className="bg-stone-50 rounded-xl border border-border-custom p-4 space-y-2">
                      <h3 className="text-xs font-bold text-text-primary font-mono">{doc.title}</h3>
                      <code className="block text-[10px] bg-white p-2 rounded border border-card/65 font-mono text-text-secondary">
                        {doc.syntax}
                      </code>
                      <p className="text-xs text-text-secondary leading-relaxed">{doc.desc}</p>
                    </div>
                  ))}
                </div>
              )}

              {activeOverlay === "Settings" && (
                <div className="space-y-4">
                  <div>
                    <label className="block text-[10px] font-bold text-text-secondary uppercase font-mono mb-1">Databricks Workspace URL</label>
                    <input
                      type="text"
                      value={workspaceUrl}
                      onChange={(e) => setWorkspaceUrl(e.target.value)}
                      className="w-full bg-stone-50 border border-border-custom p-2.5 rounded-xl text-xs text-text-primary outline-none focus:border-stone-400 font-mono"
                    />
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="block text-[10px] font-bold text-text-secondary uppercase font-mono mb-1">Catalog</label>
                      <input
                        type="text"
                        value={defaultCatalog}
                        onChange={(e) => setDefaultCatalog(e.target.value)}
                        className="w-full bg-stone-50 border border-border-custom p-2.5 rounded-xl text-xs text-text-primary outline-none focus:border-stone-400 font-mono"
                      />
                    </div>
                    <div>
                      <label className="block text-[10px] font-bold text-text-secondary uppercase font-mono mb-1">Schema</label>
                      <input
                        type="text"
                        value={defaultSchema}
                        onChange={(e) => setDefaultSchema(e.target.value)}
                        className="w-full bg-stone-50 border border-border-custom p-2.5 rounded-xl text-xs text-text-primary outline-none focus:border-stone-400 font-mono"
                      />
                    </div>
                  </div>
                  <div className="pt-2">
                    <button
                      onClick={() => {
                        setActiveOverlay(null);
                        triggerNotification("Settings saved successfully", "success");
                      }}
                      className="px-4 py-2 rounded-xl bg-black text-white text-xs font-semibold shadow hover:bg-black/90 transition cursor-pointer"
                    >
                      Save Workspace Configuration
                    </button>
                  </div>
                </div>
              )}

              {activeOverlay === "Pipeline" && (
                <div className="space-y-6">
                  <p className="text-xs text-text-secondary leading-relaxed">
                    Visual representation of the active ingestion graph. Blue outlines indicate processing or verified targets.
                  </p>
                  <div className="border border-border-custom bg-stone-50/50 rounded-xl p-4 flex flex-col items-center gap-2.5">
                    {[
                      { label: "ADLS Ingest", active: activePipelineNode === 0, completed: activePipelineNode > 0 },
                      { label: "Read Data", active: activePipelineNode === 1, completed: activePipelineNode > 1 },
                      { label: "Transform", active: activePipelineNode === 2, completed: activePipelineNode > 2 },
                      { label: "Delta Table", active: activePipelineNode === 3, completed: activePipelineNode > 3 }
                    ].map((step, idx) => {
                      return (
                        <React.Fragment key={step.label}>
                          {idx > 0 && (
                            <div className={`w-px h-5 border-l border-dashed transition-colors duration-300 ${
                              step.completed || step.active ? "border-primary" : "border-stone-300"
                            }`} />
                          )}
                          <div className={`w-full max-w-sm px-3 py-2.5 rounded-xl border text-center font-mono text-xs transition duration-200 ${
                            step.active
                              ? "bg-orange-50 border-primary text-primary font-bold shadow-sm"
                              : step.completed
                                ? "bg-stone-100 border-stone-200 text-stone-400"
                                : "bg-white border-border-custom text-text-secondary"
                          }`}>
                            {step.label}
                          </div>
                        </React.Fragment>
                      );
                    })}
                  </div>
                </div>
              )}

            </div>

          </div>
        </div>
      )}

      {/* Toast Notification */}
      {notification.show && (
        <div className={`fixed bottom-4 right-4 z-50 p-3 rounded border shadow-lg flex items-center gap-2 max-w-sm bg-white border-stone-200 text-text-primary font-mono text-xs`}>
          <div className="w-1.5 h-1.5 rounded-full bg-primary animate-pulse" />
          <span>{notification.text}</span>
        </div>
      )}

    </div>
  );
}
