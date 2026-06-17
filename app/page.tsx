"use client";

import React, { useState, useEffect, useRef } from "react";
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
  Bell,
  Menu,
  X,
  Send,
  Plus,
  MessageSquare,
  Terminal,
  Settings,
  BookOpen
} from "lucide-react";

interface Message {
  id: string;
  sender: "user" | "assistant";
  text: string;
  code?: string;
  isGenerating?: boolean;
}

const SAMPLE_PYSPARK_CODE = `from pyspark.sql import SparkSession
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
    spark = SparkSession.builder \\
        .appName("DDC_Customer_Pipeline") \\
        .getOrCreate()
        
    source_adls_path = "abfss://raw-data@datalakeprod.dfs.core.windows.net/customers/v1/"
    target_delta_table = "silver.customers_deduped"
    
    process_customer_data(spark, source_adls_path, target_delta_table)
`;

export default function DatabricksDeveloperCopilot() {
  const [inputVal, setInputVal] = useState<string>("");
  const [sidebarOpen, setSidebarOpen] = useState<boolean>(true);
  const [messages, setMessages] = useState<Message[]>([
    {
      id: "welcome",
      sender: "assistant",
      text: "Hello! I am your Databricks Developer Copilot. Describe your PySpark data ingestion, transformation, or Delta Lake requirements, and I will generate production-ready code for you."
    }
  ]);

  // Suggestions checkbox state
  const [suggestions, setSuggestions] = useState([
    { id: "logging", text: "Add logging", active: true },
    { id: "delta_opt", text: "Use Delta optimization", active: false },
    { id: "cache", text: "Cache DataFrame", active: false }
  ]);

  const [copiedId, setCopiedId] = useState<string | null>(null);
  const [generatingId, setGeneratingId] = useState<string | null>(null);
  const [generationLinesCount, setGenerationLinesCount] = useState<number>(0);

  const [notification, setNotification] = useState<{ show: boolean; text: string; type: "success" | "info" }>({
    show: false,
    text: "",
    type: "info"
  });

  const triggerNotification = (text: string, type: "success" | "info" = "info") => {
    setNotification({ show: true, text, type });
    setTimeout(() => {
      setNotification(prev => ({ ...prev, show: false }));
    }, 3000);
  };

  const chatEndRef = useRef<HTMLDivElement>(null);

  // Auto Scroll to Bottom of Chat on new messages
  useEffect(() => {
    chatEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, generationLinesCount]);

  const handleSend = () => {
    if (!inputVal.trim() || generatingId) return;

    const userPrompt = inputVal;
    setInputVal(""); // Clear prompt box

    const userMsgId = `user-${Date.now()}`;
    const assistantMsgId = `assistant-${Date.now()}`;

    // 1. Add User query message to log
    setMessages(prev => [
      ...prev,
      { id: userMsgId, sender: "user", text: userPrompt }
    ]);

    // 2. Add Assistant template response
    setTimeout(() => {
      setMessages(prev => [
        ...prev,
        {
          id: assistantMsgId,
          sender: "assistant",
          text: `Here is the production-ready PySpark script designed to handle your requirement:`,
          code: "",
          isGenerating: true
        }
      ]);

      setGeneratingId(assistantMsgId);
      setGenerationLinesCount(0);

      // Customize output code depending on Suggestions checkboxes
      let finalCode = SAMPLE_PYSPARK_CODE;
      const loggingActive = suggestions.find(s => s.id === "logging")?.active;
      const deltaActive = suggestions.find(s => s.id === "delta_opt")?.active;
      const cacheActive = suggestions.find(s => s.id === "cache")?.active;

      if (!loggingActive) {
        finalCode = finalCode
          .replace("import logging", "")
          .replace(/logger\.\w+\(.*?\)/g, "# logging disabled");
      }
      if (cacheActive) {
        finalCode = finalCode.replace(
          "df_deduped = df_raw",
          "df_deduped = df_raw.cache() # Cache raw data for reuse"
        );
      }
      if (deltaActive) {
        finalCode = finalCode.replace(
          "logger.info(\"Pipeline executed successfully!\")",
          `# Run Delta optimization\n    logger.info("Optimizing Delta Lake table layout...")\n    spark.sql(f"OPTIMIZE {target_table} ZORDER BY (customer_id)")\n    logger.info("Pipeline executed successfully!")`
        );
      }

      const lines = finalCode.split("\n");
      let currentLine = 0;

      // Simulate streaming lines typing
      const interval = setInterval(() => {
        currentLine += 2; // Stream 2 lines at a time for smooth speed
        if (currentLine > lines.length) {
          currentLine = lines.length;
        }

        setGenerationLinesCount(currentLine);

        // Update assistant code stream state
        setMessages(prev =>
          prev.map(msg => {
            if (msg.id === assistantMsgId) {
              return {
                ...msg,
                code: lines.slice(0, currentLine).join("\n")
              };
            }
            return msg;
          })
        );

        if (currentLine >= lines.length) {
          clearInterval(interval);
          setGeneratingId(null);
          setMessages(prev =>
            prev.map(msg => {
              if (msg.id === assistantMsgId) {
                return { ...msg, isGenerating: false };
              }
              return msg;
            })
          );
        }
      }, 50);

    }, 600);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const toggleSuggestion = (id: string) => {
    setSuggestions(prev =>
      prev.map(s => (s.id === id ? { ...s, active: !s.active } : s))
    );
  };

  const handleCopyCode = (codeText: string, msgId: string) => {
    navigator.clipboard.writeText(codeText);
    setCopiedId(msgId);
    setTimeout(() => setCopiedId(null), 2000);
  };

  const handleDownloadCode = (codeText: string) => {
    const element = document.createElement("a");
    const file = new Blob([codeText], { type: "text/plain" });
    element.href = URL.createObjectURL(file);
    element.download = "process_customer_data.py";
    document.body.appendChild(element);
    element.click();
    document.body.removeChild(element);
  };

  // Custom Python syntax highlighter for light editor box
  const highlightCode = (codeText: string) => {
    const rawLines = codeText.split("\n");
    return rawLines.map((line, idx) => {
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
        highlighted = highlighted.replace(regex, `<span class="text-[#EA580C] font-semibold">${kw}</span>`);
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
          <span className="pl-4 flex-1 whitespace-pre text-stone-800" dangerouslySetInnerHTML={{ __html: highlighted }} />
        </div>
      );
    });
  };

  return (
    <div className="h-screen w-screen flex overflow-hidden bg-background text-text-primary">
      
      {/* 1. LEFT SIDEBAR (Dark Navy) */}
      <aside className={`bg-sidebar text-slate-300 flex flex-col justify-between border-r border-border-custom z-20 shrink-0 transition-transform duration-300 lg:translate-x-0 ${
        sidebarOpen ? "w-64 translate-x-0 static" : "w-0 -translate-x-full fixed"
      }`}>
        
        {/* Sidebar content */}
        <div className="flex flex-col h-full overflow-hidden">
          
          {/* Header block */}
          <div className="h-14 flex items-center justify-between px-4 border-b border-slate-800 bg-slate-950/40 shrink-0">
            <div className="flex items-center gap-2">
              <div className="w-5 h-5 rounded bg-primary flex items-center justify-center text-[10px] font-bold text-white">
                D
              </div>
              <span className="font-semibold text-xs text-white uppercase tracking-wider">Dev Copilot</span>
            </div>
            <button 
              onClick={() => setSidebarOpen(false)}
              className="p-1 rounded hover:bg-slate-800 text-slate-400 hover:text-white"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          {/* New Chat Session CTA */}
          <div className="p-3 shrink-0">
            <button 
              onClick={() => {
                setMessages([
                  {
                    id: "welcome",
                    sender: "assistant",
                    text: "Hello! Describe your PySpark data ingestion, transformation, or Delta Lake requirements, and I will generate production-ready code."
                  }
                ]);
              }}
              className="w-full py-2 px-3 rounded-lg border border-slate-700 hover:border-slate-500 text-white text-xs font-semibold flex items-center justify-center gap-2 hover:bg-slate-900 transition"
            >
              <Plus className="w-4 h-4" />
              <span>New Session</span>
            </button>
          </div>

          {/* Past Chat Session list */}
          <div className="flex-1 overflow-y-auto px-2 py-1 space-y-1 select-none">
            <div className="px-3 py-1.5 text-[10px] font-bold text-slate-500 uppercase tracking-widest">Recent Sessions</div>
            {[
              "Raw Ingestion Customers",
              "Sales Category Rollups",
              "Z-Order Delta Optimizations",
              "Window Rank Deduplication"
            ].map((session, idx) => (
              <button
                key={idx}
                onClick={() => {
                  setInputVal(`Read and process ${session.toLowerCase()} table...`);
                  handleSend();
                }}
                className="w-full py-2 px-3 rounded-md text-left text-xs text-slate-400 hover:text-white hover:bg-slate-900/60 truncate flex items-center gap-2 transition"
              >
                <MessageSquare className="w-3.5 h-3.5 shrink-0" />
                <span className="truncate">{session}</span>
              </button>
            ))}
          </div>

        </div>

        {/* Sidebar Footer */}
        <div className="p-4 border-t border-slate-800 bg-slate-950/20 shrink-0">
          <div className="flex items-center gap-2 text-xs font-mono text-slate-400">
            <span className="w-2 h-2 rounded-full bg-success animate-pulse" />
            <span>Lakehouse Online</span>
          </div>
        </div>

      </aside>

      {/* Main Chat Frame */}
      <div className="flex-1 flex flex-col overflow-hidden min-w-0 bg-white">
        
        {/* TOP HEADER */}
        <header className="h-14 bg-white border-b border-border-custom flex items-center justify-between px-6 shrink-0 sticky top-0 z-10">
          <div className="flex items-center gap-3">
            {!sidebarOpen && (
              <button
                onClick={() => setSidebarOpen(true)}
                className="p-1.5 rounded hover:bg-slate-100 border border-slate-200 text-slate-600"
              >
                <Menu className="w-4 h-4" />
              </button>
            )}
            <span className="font-semibold text-sm text-text-primary">
              Databricks Developer Copilot
            </span>
          </div>

          <div className="flex items-center gap-2">
            <span className="text-[10px] font-mono text-text-secondary bg-stone-100 px-2 py-0.5 rounded border border-border-custom">
              Cluster: standard_d8s_v3
            </span>
            <div className="w-7 h-7 rounded-full bg-slate-200 flex items-center justify-center font-bold text-[10px] text-slate-700 border border-border-custom shadow-sm">
              DE
            </div>
          </div>
        </header>

        {/* Scrollable Message Flow Area */}
        <div className="flex-1 overflow-y-auto px-4 md:px-8 py-8 space-y-8 select-text">
          
          <div className="max-w-3xl mx-auto space-y-8">
            {messages.map((msg) => {
              const isAssistant = msg.sender === "assistant";
              return (
                <div key={msg.id} className="flex gap-4 items-start text-left">
                  
                  {/* Sender Icon */}
                  <div className={`w-8 h-8 rounded-full shrink-0 flex items-center justify-center font-bold text-xs shadow-sm border ${
                    isAssistant 
                      ? "bg-orange-50 border-orange-200 text-primary" 
                      : "bg-stone-100 border-stone-200 text-slate-700"
                  }`}>
                    {isAssistant ? <Sparkles className="w-4 h-4" /> : "ME"}
                  </div>

                  {/* Message body */}
                  <div className="flex-1 space-y-4">
                    
                    {/* Prompt message explanation */}
                    <div className="text-base text-text-primary leading-relaxed whitespace-pre-wrap">
                      {msg.text}
                    </div>

                    {/* Embedded code editor if available */}
                    {msg.code !== undefined && (
                      <div className="rounded-xl border border-border-custom shadow-sm overflow-hidden flex flex-col bg-white">
                        
                        {/* Editor Header */}
                        <div className="px-4 py-2 bg-stone-50 border-b border-border-custom flex items-center justify-between shrink-0">
                          <div className="flex items-center gap-2">
                            <FileCode className="w-4 h-4 text-stone-500" />
                            <span className="font-mono text-xs font-semibold text-text-primary">process_customer_data.py</span>
                          </div>
                          
                          {/* Code actions */}
                          <div className="flex items-center gap-1">
                            <button
                              onClick={() => handleCopyCode(msg.code || "", msg.id)}
                              className="p-1 rounded hover:bg-stone-200 text-text-secondary hover:text-text-primary transition"
                              title="Copy Code"
                            >
                              {copiedId === msg.id ? <Check className="w-3.5 h-3.5 text-success" /> : <Copy className="w-3.5 h-3.5" />}
                            </button>
                            <button
                              onClick={() => handleDownloadCode(msg.code || "")}
                              className="p-1 rounded hover:bg-stone-200 text-text-secondary hover:text-text-primary transition"
                              title="Download Script"
                            >
                              <Download className="w-3.5 h-3.5" />
                            </button>
                          </div>
                        </div>

                        {/* Editor content block */}
                        <div className="flex-1 py-4 overflow-y-auto max-h-[360px] flex flex-col bg-white">
                          {highlightCode(msg.code)}

                          {msg.isGenerating && (
                            <div className="px-4 flex items-center py-1 font-mono text-[10px] text-text-secondary/60 animate-pulse">
                              <span className="w-10 text-right pr-4 border-r border-slate-200 select-none">···</span>
                              <span className="pl-4 italic flex items-center gap-1">
                                <RefreshCw className="w-3 h-3 animate-spin text-primary" /> Building Spark DataFrames...
                              </span>
                            </div>
                          )}
                        </div>

                      </div>
                    )}

                  </div>

                </div>
              );
            })}
          </div>

          <div ref={chatEndRef} />
        </div>

        {/* 2. CHAT INPUT PANEL (Bottom, Sticky) */}
        <div className="bg-gradient-to-t from-white via-white/95 to-transparent shrink-0">
          
          <div className="max-w-3xl w-full mx-auto px-4 pb-6 pt-2 text-left">
            
            {/* Input bar */}
            <div className="bg-[#F4F4F4] border border-stone-200 rounded-2xl shadow-sm p-2 flex flex-col relative focus-within:border-stone-300 transition-all duration-200">
              
              <textarea
                value={inputVal}
                onChange={(e) => setInputVal(e.target.value)}
                onKeyDown={handleKeyDown}
                rows={1}
                placeholder="Ask me to generate PySpark code or troubleshoot errors... (Press Enter to send)"
                className="w-full min-h-[44px] bg-transparent text-base border-0 focus:ring-0 outline-none resize-none leading-relaxed p-2 text-text-primary placeholder-stone-400"
              />

              {/* Action/toggles footer container */}
              <div className="px-2 pt-2 pb-1 border-t border-stone-200/50 flex flex-col sm:flex-row sm:items-center justify-between gap-3 select-none">
                
                {/* Suggestions checkbox */}
                <div className="flex flex-wrap gap-4 items-center">
                  {suggestions.map((item) => (
                    <label key={item.id} className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={item.active}
                        onChange={() => toggleSuggestion(item.id)}
                        className="rounded border-stone-300 bg-white text-primary focus:ring-0 w-3.5 h-3.5"
                      />
                      <span className={`text-xs font-sans transition ${item.active ? "text-text-primary font-medium" : "text-text-secondary"}`}>
                        {item.text}
                      </span>
                    </label>
                  ))}
                </div>

                {/* Send button (Orange style) */}
                <button
                  onClick={handleSend}
                  disabled={!inputVal.trim() || !!generatingId}
                  className={`self-end sm:self-auto p-2 rounded-xl text-white transition cursor-pointer flex items-center justify-center ${
                    inputVal.trim() && !generatingId
                      ? "bg-primary hover:bg-primary/90"
                      : "bg-stone-300 text-stone-500 cursor-not-allowed"
                  }`}
                  title="Send Message"
                >
                  <Send className="w-4 h-4" />
                </button>
              </div>

            </div>

            {/* Shift+Enter helper warning */}
            <div className="text-[10px] text-stone-400 mt-2 pl-2 text-center sm:text-left font-sans">
              Press <kbd className="px-1.5 py-0.5 rounded bg-stone-100 border border-stone-200 text-stone-500 select-none">Enter</kbd> to generate code • Use <kbd className="px-1.5 py-0.5 rounded bg-stone-100 border border-stone-200 text-stone-500 select-none">Shift + Enter</kbd> for new lines
            </div>

          </div>

        </div>

      </div>

      {/* Toast Notification */}
      {notification.show && (
        <div className={`fixed bottom-4 right-4 z-50 p-3 rounded border shadow-lg flex items-center gap-2 max-w-sm transition-all duration-300 ${
          notification.type === "success" 
            ? "bg-green-50 border-green-200 text-green-800 font-mono text-xs" 
            : "bg-white border-stone-200 text-text-primary font-mono text-xs"
        }`}>
          <div className={`w-1.5 h-1.5 rounded-full ${notification.type === "success" ? "bg-green-600" : "bg-primary"} animate-pulse`} />
          <span>{notification.text}</span>
        </div>
      )}

    </div>
  );
}
