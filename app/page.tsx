"use client";

import React, { useState, useEffect, useRef } from "react";
import {
  Play,
  Copy,
  Download,
  Maximize2,
  Minimize2,
  ChevronLeft,
  ChevronRight,
  Search,
  Bell,
  User,
  Terminal,
  Folder,
  Settings,
  BookOpen,
  Cpu,
  Database,
  Sparkles,
  AlertTriangle,
  Check,
  FileCode,
  Layers,
  ArrowRight,
  RefreshCw,
  Clock,
  ExternalLink,
  ChevronDown
} from "lucide-react";

// Definitions for the preloaded notebooks
interface Notebook {
  id: string;
  name: string;
  prompt: string;
  code: string;
  pipelineNodes: string[];
}

const NOTEBOOKS: Notebook[] = [
  {
    id: "raw_ingest_customers.py",
    name: "raw_ingest_customers.py",
    prompt: "Read customer data from ADLS, remove duplicates using latest timestamp, and load into a Delta table.",
    pipelineNodes: ["ADLS Ingest", "Read JSON/CSV", "Deduplicate", "Delta Silver Table"],
    code: `from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
import logging

# Set up logging for production traceability
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DatabricksDeveloperCopilot")

def process_customer_data(spark: SparkSession, source_path: str, target_table: str):
    logger.info(f"Reading raw customer data from ADLS: {source_path}")
    
    # 1. Read customer data from ADLS with schema inference
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
    
    # 3. Write data to Delta Lake table with merge schema option enabled
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
  },
  {
    id: "aggregate_sales.py",
    name: "aggregate_sales.py",
    prompt: "Read sales Delta table, compute daily revenue and running totals, and save to a gold table.",
    pipelineNodes: ["Silver Sales Table", "Filter Active", "Sum & Window", "Gold Sales Table"],
    code: `from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SalesAggregator")

def aggregate_sales(spark: SparkSession, source_table: str, target_table: str):
    logger.info(f"Loading bronze sales data from table: {source_table}")
    
    # 1. Read sales transactional data
    df_sales = spark.read.table(source_table)
    
    logger.info("Computing daily revenue aggregated by product category...")
    
    # 2. Group by date and category, sum revenue and transaction count
    df_daily_revenue = df_sales \\
        .groupBy("sale_date", "product_category") \\
        .agg(
            sum("revenue").alias("daily_revenue"),
            count("transaction_id").alias("transaction_count")
        )
        
    logger.info("Calculating cumulative running total revenue per category...")
    
    # 3. Window spec for cumulative running total sorted by date
    window_cum = Window.partitionBy("product_category").orderBy("sale_date") \\
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
    df_with_running_total = df_daily_revenue \\
        .withColumn("running_total_revenue", sum("daily_revenue").over(window_cum))
        
    logger.info(f"Saving gold sales metrics to table: {target_table}")
    
    # 4. Save to Delta table
    df_with_running_total.write \\
        .format("delta") \\
        .mode("overwrite") \\
        .option("mergeSchema", "true") \\
        .saveAsTable(target_table)
        
    logger.info("Sales aggregation pipeline completed successfully!")

if __name__ == "__main__":
    spark = SparkSession.builder \\
        .appName("Sales_Aggregation_Pipeline") \\
        .getOrCreate()
        
    aggregate_sales(spark, "silver.sales_transactions", "gold.daily_sales_analytics")
`
  },
  {
    id: "optimize_zorder.py",
    name: "optimize_zorder.py",
    prompt: "Read transaction logs, partition by transaction_date, and optimize Delta table using Z-order by customer_id.",
    pipelineNodes: ["Delta Storage", "Check Partition", "Vacuum Log", "Z-Order Optimization"],
    code: `from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeltaOptimizer")

def run_delta_optimization(spark: SparkSession, table_name: str, partition_col: str, zorder_col: str):
    logger.info(f"Checking Delta table stats for: {table_name}")
    
    # 1. Load active data to verify metadata validity
    df = spark.read.table(table_name)
    logger.info(f"Record count before optimize: {df.count()}")
    
    # 2. Perform Z-Order Optimization on target table
    logger.info(f"Optimizing Delta table: {table_name} Z-ordered by {zorder_col}")
    
    # Delta optimization command executed via Spark SQL
    optimize_query = f"OPTIMIZE {table_name} ZORDER BY ({zorder_col})"
    spark.sql(optimize_query)
    
    # 3. Vacuum table to clean up files older than default retention (168 hours)
    logger.info("Running VACUUM to remove stale transaction log histories...")
    spark.sql(f"VACUUM {table_name} RETAIN 168 HOURS")
    
    logger.info("Delta Lake optimization process completed successfully.")

if __name__ == "__main__":
    spark = SparkSession.builder \\
        .appName("Delta_ZOrder_Optimization") \\
        .getOrCreate()
        
    run_delta_optimization(spark, "silver.customer_transactions", "transaction_date", "customer_id")
`
  }
];

export default function DatabricksDeveloperCopilot() {
  // Navigation tabs
  const [activeTab, setActiveTab] = useState<string>("Code Generator");
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(false);
  const [rightSidebarCollapsed, setRightSidebarCollapsed] = useState<boolean>(false);
  const [isFullScreen, setIsFullScreen] = useState<boolean>(false);

  // Command palette state
  const [isPaletteOpen, setIsPaletteOpen] = useState<boolean>(false);
  const [paletteSearch, setPaletteSearch] = useState<string>("");

  // Notebook content state
  const [selectedNotebookId, setSelectedNotebookId] = useState<string>(NOTEBOOKS[0].id);
  const currentNotebook = NOTEBOOKS.find(n => n.id === selectedNotebookId) || NOTEBOOKS[0];

  const [prompt, setPrompt] = useState<string>(currentNotebook.prompt);
  const [editorCode, setEditorCode] = useState<string>(currentNotebook.code);

  // Generation state
  const [isGenerating, setIsGenerating] = useState<boolean>(false);
  const [generationLineIndex, setGenerationLineIndex] = useState<number>(currentNotebook.code.split("\n").length);
  const [activePipelineNode, setActivePipelineNode] = useState<number>(3); // 0, 1, 2, 3

  // Suggestions state
  const [suggestions, setSuggestions] = useState([
    { id: "logging", text: "Add logging", active: true, desc: "Include python logging statements for production debugging." },
    { id: "delta_opt", text: "Use Delta optimization", active: false, desc: "Run OPTIMIZE & VACUUM commands on targets." },
    { id: "broadcast", text: "Consider broadcast joins", active: false, desc: "Optimize small joins using PySpark broadcast function." },
    { id: "cache", text: "Cache intermediate DataFrames", active: false, desc: "Use .cache() on repeated tables." }
  ]);

  // Notifications
  const [copied, setCopied] = useState<boolean>(false);
  const [notification, setNotification] = useState<{ show: boolean; text: string; type: "success" | "info" }>({
    show: false,
    text: "",
    type: "info"
  });

  // UI Theme (Midnight Data Lab vs Matrix Terminal)
  const [matrixTheme, setMatrixTheme] = useState<boolean>(false);

  // Error Analyzer state
  const [errorInput, setErrorInput] = useState<string>("");
  const [errorDiagnosis, setErrorDiagnosis] = useState<any | null>(null);
  const [isAnalyzingError, setIsAnalyzingError] = useState<boolean>(false);

  // References for scroll lock
  const editorEndRef = useRef<HTMLDivElement>(null);

  // Update prompt/code when notebook changes
  useEffect(() => {
    setPrompt(currentNotebook.prompt);
    setEditorCode(currentNotebook.code);
    setGenerationLineIndex(currentNotebook.code.split("\n").length);
    setActivePipelineNode(3);
  }, [selectedNotebookId]);

  // Command palette keyboard listener
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setIsPaletteOpen(prev => !prev);
      }
      if (e.key === "Escape") {
        setIsPaletteOpen(false);
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  // Show dynamic notification
  const triggerNotification = (text: string, type: "success" | "info" = "info") => {
    setNotification({ show: true, text, type });
    setTimeout(() => {
      setNotification(prev => ({ ...prev, show: false }));
    }, 4000);
  };

  // Run PySpark code generation stream simulation
  const handleGenerate = () => {
    if (isGenerating) return;

    setIsGenerating(true);
    setGenerationLineIndex(0);
    setActivePipelineNode(0);
    triggerNotification("Starting PySpark code compilation...", "info");

    const fullCode = currentNotebook.code;
    const lines = fullCode.split("\n");
    let currentLine = 0;

    // Suggesion injection customization logic
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
        "df_deduped = df_raw.cache() # Cache raw dataframe"
      );
    }

    if (deltaActive) {
      customizedCode = customizedCode.replace(
        "logger.info(\"Pipeline executed successfully!\")",
        `# Delta Optimization Z-order\n    logger.info("Running OPTIMIZE on target table...")\n    spark.sql(f"OPTIMIZE {target_table} ZORDER BY (customer_id)")\n    logger.info("Pipeline executed successfully!")`
      );
    }

    const customLines = customizedCode.split("\n");

    const interval = setInterval(() => {
      currentLine++;
      setGenerationLineIndex(currentLine);

      // Dynamic pipeline nodes transition
      const progressRatio = currentLine / customLines.length;
      if (progressRatio < 0.25) {
        setActivePipelineNode(0);
      } else if (progressRatio < 0.5) {
        setActivePipelineNode(1);
      } else if (progressRatio < 0.8) {
        setActivePipelineNode(2);
      } else {
        setActivePipelineNode(3);
      }

      if (currentLine >= customLines.length) {
        clearInterval(interval);
        setIsGenerating(false);
        setEditorCode(customizedCode);
        triggerNotification("PySpark code generated successfully!", "success");
      }
    }, 45); // Smooth fast typing
  };

  // Toggle Suggestions
  const toggleSuggestion = (id: string) => {
    setSuggestions(prev =>
      prev.map(s => (s.id === id ? { ...s, active: !s.active } : s))
    );
    triggerNotification(`Updated suggestion settings: ${suggestions.find(s => s.id === id)?.text}`, "info");
  };

  // Copy Code to Clipboard
  const handleCopyCode = () => {
    navigator.clipboard.writeText(editorCode);
    setCopied(true);
    triggerNotification("Code copied to clipboard", "success");
    setTimeout(() => setCopied(false), 2000);
  };

  // Download Code as python file
  const handleDownloadCode = () => {
    const element = document.createElement("a");
    const file = new Blob([editorCode], { type: "text/plain" });
    element.href = URL.createObjectURL(file);
    element.download = currentNotebook.id;
    document.body.appendChild(element);
    element.click();
    document.body.removeChild(element);
    triggerNotification(`Downloaded ${currentNotebook.id} file`, "success");
  };

  // Custom Python Syntax Highlighter
  const renderHighlightedCode = () => {
    const rawLines = editorCode.split("\n");
    const activeLines = rawLines.slice(0, generationLineIndex);

    return activeLines.map((line, idx) => {
      // Return empty line spacer
      if (line.trim() === "") {
        return (
          <div key={idx} className="flex min-h-[1.5rem] select-none hover:bg-card/45 px-4 font-mono text-xs">
            <span className="w-10 text-right pr-4 text-textSecondary opacity-30 select-none border-r border-card/60">{idx + 1}</span>
            <span className="pl-4">&nbsp;</span>
          </div>
        );
      }

      // Escape HTML entities to prevent rendering breaks
      let highlighted = line
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");

      // Comments: # ...
      const commentRegex = /(#.*)$/g;
      highlighted = highlighted.replace(commentRegex, '<span class="text-gray-500 italic">$1</span>');

      // Strings: "..." or '...'
      highlighted = highlighted.replace(/(".*?")/g, '<span class="text-success">$1</span>');
      highlighted = highlighted.replace(/('.*?')/g, '<span class="text-success">$1</span>');

      // Python Keywords
      const keywords = [
        "def", "class", "import", "from", "as", "return", "if", "else", "elif",
        "try", "except", "finally", "while", "for", "in", "and", "or", "not",
        "with", "pass", "True", "False", "None", "print", "main"
      ];
      keywords.forEach(kw => {
        const regex = new RegExp(`\\b${kw}\\b`, "g");
        highlighted = highlighted.replace(regex, `<span class="text-primary font-semibold">${kw}</span>`);
      });

      // Databricks / Spark terms
      const sparkKeywords = [
        "SparkSession", "Window", "col", "row_number", "desc", "partitionBy", "orderBy", "spark", "logger"
      ];
      sparkKeywords.forEach(skw => {
        const regex = new RegExp(`\\b${skw}\\b`, "g");
        highlighted = highlighted.replace(regex, `<span class="text-secondary font-medium">${skw}</span>`);
      });

      // PySpark operations
      const sparkOperations = [
        "read", "write", "format", "option", "load", "mode", "filter", "select",
        "withColumn", "drop", "saveAsTable", "over", "builder", "appName",
        "getOrCreate", "basicConfig", "getLogger", "info", "error"
      ];
      sparkOperations.forEach(so => {
        const regex = new RegExp(`\\b${so}\\b`, "g");
        highlighted = highlighted.replace(regex, `<span class="text-[#A578FF]">${so}</span>`);
      });

      const isLastLine = idx === activeLines.length - 1;

      return (
        <div key={idx} className={`flex hover:bg-card/45 px-4 font-mono text-xs ${isGenerating && isLastLine ? "bg-primary/5" : ""}`}>
          <span className="w-10 text-right pr-4 text-textSecondary opacity-40 select-none border-r border-card/60">{idx + 1}</span>
          <span
            className={`pl-4 flex-1 whitespace-pre ${isGenerating && isLastLine ? "cursor-blink-line" : ""}`}
            dangerouslySetInnerHTML={{ __html: highlighted }}
          />
        </div>
      );
    });
  };

  // Spark Error diagnosis templates
  const SAMPLE_SPARK_ERROR = `org.apache.spark.sql.AnalysisException: Table or view not found: silver.sales_transactions; line 14 pos 25;
at org.apache.spark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)
at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations$.resolveRelation(Analyzer.scala:224)
at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations$.apply(Analyzer.scala:125)
at org.apache.spark.sql.catalyst.analysis.Analyzer$ResolveRelations$.apply(Analyzer.scala:114)
at org.apache.spark.sql.catalyst.rules.RuleExecutor.$anonfun$execute$1(RuleExecutor.scala:68)
... 25 more`;

  const handleAnalyzeError = () => {
    if (!errorInput.trim()) {
      triggerNotification("Please enter or load a traceback log", "info");
      return;
    }
    setIsAnalyzingError(true);
    setTimeout(() => {
      setIsAnalyzingError(false);
      setErrorDiagnosis({
        errorCode: "SPARK_TABLE_NOT_FOUND (AnalysisException)",
        severity: "CRITICAL",
        rootCause: "Spark cannot locate the table 'silver.sales_transactions' in the catalog. This typically occurs because Unity Catalog credentials are not loaded, the catalog namespace is omitted, or the table has not been ingested yet.",
        fixes: [
          { title: "Verify Active Catalog & Schema", code: 'spark.sql("USE CATALOG main")\nspark.sql("SHOW TABLES IN silver")' },
          { title: "Verify Table Existence", code: 'spark.catalog.tableExists("silver.sales_transactions")' },
          { title: "Review Schema Definition", code: '# Ingest table first using bronze layer\ndf.write.format("delta").saveAsTable("silver.sales_transactions")' }
        ]
      });
      triggerNotification("Error analysis complete", "success");
    }, 1200);
  };

  return (
    <div className={`h-screen w-screen flex flex-col ${matrixTheme ? "font-mono" : "font-sans"} text-textPrimary overflow-hidden bg-background`}>
      
      {/* 2. Top Header */}
      <header className="h-14 bg-surface border-b border-card flex items-center justify-between px-4 sticky top-0 z-30">
        
        {/* Left branding */}
        <div className="flex items-center gap-3">
          <div className="flex items-center justify-center w-8 h-8 rounded-md bg-gradient-to-tr from-primary to-orange-500 shadow-md">
            <Layers className="w-4.5 h-4.5 text-white" />
          </div>
          <div className="flex flex-col">
            <span className="font-display font-bold text-sm tracking-tight text-textPrimary">
              Databricks Developer Copilot
            </span>
            <span className="text-[10px] text-textSecondary font-mono -mt-1">
              v1.0.4 • Midnight Data Lab
            </span>
          </div>
        </div>

        {/* Global Command Search bar */}
        <div className="hidden md:flex items-center max-w-lg w-full px-3 py-1.5 rounded bg-background border border-card/80 hover:border-card text-textSecondary cursor-pointer gap-2 transition"
             onClick={() => setIsPaletteOpen(true)}>
          <Search className="w-4 h-4" />
          <span className="text-xs text-textSecondary flex-1">Search notebooks, error logs, or commands...</span>
          <kbd className="px-1.5 py-0.5 rounded bg-card text-[9px] font-mono border border-card/50">Ctrl K</kbd>
        </div>

        {/* Actions panel */}
        <div className="flex items-center gap-3">
          
          {/* Notification Button */}
          <button className="p-2 rounded bg-card hover:bg-card/75 text-textSecondary hover:text-textPrimary transition relative"
                  onClick={() => triggerNotification("No new system warnings or cluster logs.", "info")}>
            <Bell className="w-4 h-4" />
            <span className="absolute top-1.5 right-1.5 w-2 h-2 rounded-full bg-primary animate-pulse" />
          </button>

          {/* Custom Theme toggle (Midnight vs Matrix) */}
          <button 
            onClick={() => {
              setMatrixTheme(prev => !prev);
              triggerNotification(`Switched interface to ${!matrixTheme ? "Matrix Green" : "Midnight Data"} theme`, "success");
            }}
            className={`px-2.5 py-1.5 text-xs rounded border transition flex items-center gap-1.5 ${
              matrixTheme 
                ? "bg-emerald-950/40 border-emerald-500/50 text-emerald-400 hover:bg-emerald-900/40" 
                : "bg-card border-card hover:bg-card/70 text-textSecondary"
            }`}
          >
            <Terminal className="w-3.5 h-3.5" />
            <span className="hidden sm:inline">{matrixTheme ? "Matrix Theme" : "Midnight Theme"}</span>
          </button>

          {/* Divider */}
          <div className="w-px h-6 bg-card" />

          {/* User Profile */}
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 rounded bg-gradient-to-br from-secondary to-blue-600 flex items-center justify-center font-bold text-xs shadow text-white">
              DE
            </div>
            <div className="hidden lg:flex flex-col text-left">
              <span className="text-xs font-semibold leading-none text-textPrimary">Data Eng</span>
              <span className="text-[10px] text-textSecondary font-mono mt-0.5">Cluster: Active</span>
            </div>
          </div>
        </div>
      </header>

      {/* Main Workspace Frame */}
      <div className="flex-1 flex overflow-hidden">
        
        {/* 1. Left Sidebar */}
        <aside className={`bg-surface border-r border-card flex flex-col justify-between transition-all duration-300 ease-in-out z-20 ${
          sidebarCollapsed ? "w-14" : "w-60"
        }`}>
          {/* Sidebar Nav Items */}
          <div className="py-4 flex-1 flex flex-col gap-1">
            
            {/* Collapse Toggle */}
            <div className={`flex px-3 mb-4 ${sidebarCollapsed ? "justify-center" : "justify-between items-center"}`}>
              {!sidebarCollapsed && <span className="text-[11px] font-bold text-textSecondary uppercase tracking-widest pl-1 font-mono">Workspace Nav</span>}
              <button 
                onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
                className="p-1.5 rounded hover:bg-card text-textSecondary hover:text-textPrimary transition"
              >
                {sidebarCollapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
              </button>
            </div>

            {/* Menu Items */}
            {[
              { name: "Code Generator", icon: FileCode, desc: "PySpark AI cell generator" },
              { name: "Notebook Explorer", icon: Folder, desc: "Browse workspace notebooks" },
              { name: "Error Analyzer", icon: AlertTriangle, desc: "Debug Spark execution trace logs" },
              { name: "Documentation", icon: BookOpen, desc: "PySpark & Delta API guides" },
              { name: "Knowledge Search", icon: Search, desc: "Search lakehouse patterns" },
              { name: "Settings", icon: Settings, desc: "Configure cluster and endpoints" }
            ].map(item => {
              const Icon = item.icon;
              const isActive = activeTab === item.name;

              return (
                <button
                  key={item.name}
                  onClick={() => {
                    setActiveTab(item.name);
                    if (isFullScreen) setIsFullScreen(false);
                  }}
                  className={`relative group w-full py-2.5 px-3.5 flex items-center transition duration-150 ${
                    isActive 
                      ? "text-primary bg-card/60 font-semibold" 
                      : "text-textSecondary hover:text-textPrimary hover:bg-card/25"
                  }`}
                >
                  {/* Left Border Active bar */}
                  {isActive && (
                    <span className="absolute left-0 top-0 bottom-0 w-1 bg-primary" />
                  )}

                  <Icon className={`w-4.5 h-4.5 min-w-[1.25rem] ${isActive ? "text-primary" : "text-textSecondary group-hover:text-textPrimary"}`} />
                  
                  {!sidebarCollapsed && (
                    <div className="ml-3 flex flex-col text-left">
                      <span className="text-xs leading-none">{item.name}</span>
                    </div>
                  )}

                  {/* Collapsed Tooltip helper */}
                  {sidebarCollapsed && (
                    <div className="absolute left-16 py-1 px-2.5 rounded bg-card border border-card shadow-lg text-[10px] whitespace-nowrap text-textPrimary hidden group-hover:block z-50">
                      {item.name}
                    </div>
                  )}
                </button>
              );
            })}
          </div>

          {/* Cluster Status Footer indicator */}
          <div className="p-3 border-t border-card bg-background/30">
            <div className={`flex items-center ${sidebarCollapsed ? "justify-center" : "gap-3"}`}>
              <div className="w-2.5 h-2.5 rounded-full bg-success animate-pulse" />
              {!sidebarCollapsed && (
                <div className="flex flex-col">
                  <span className="text-[10px] font-bold text-textPrimary font-mono leading-none">Lakehouse_Cluster_4.2</span>
                  <span className="text-[9px] text-textSecondary font-mono mt-0.5">16 Workers • Standard_D8s_v3</span>
                </div>
              )}
            </div>
          </div>
        </aside>

        {/* Dynamic Inner Panel Viewport */}
        <main className={`flex-1 flex overflow-hidden relative`}>
          
          {/* Main workspace layout depending on Active Tab */}
          
          {/* Tab 1: Code Generator */}
          {activeTab === "Code Generator" && (
            <div className="flex-1 flex flex-col lg:flex-row overflow-hidden">
              
              {/* Left Main Workspace Panel */}
              <div className="flex-1 flex flex-col overflow-y-auto p-6 space-y-6">
                
                {/* Header title */}
                <div className="space-y-1">
                  <h2 className="text-2xl md:text-3xl font-display font-extrabold tracking-tight text-textPrimary flex items-center gap-2.5">
                    What do you want to build today?
                    <Sparkles className="w-5 h-5 text-primary" />
                  </h2>
                  <p className="text-sm text-textSecondary max-w-2xl leading-relaxed">
                    Describe your Databricks requirement and generate production-ready PySpark code.
                  </p>
                </div>

                {/* 3. Main Workspace Notebook Input Area */}
                <div className="rounded-lg border border-card bg-surface overflow-hidden shadow-xl hover:border-card/80 transition-all duration-300">
                  {/* Notebook style header cell bar */}
                  <div className="px-4 py-2 bg-card/65 border-b border-card flex items-center justify-between font-mono text-[11px] text-textSecondary">
                    <div className="flex items-center gap-2.5">
                      <span className="w-2.5 h-2.5 rounded-full bg-primary/20 border border-primary/45" />
                      <span className="font-bold text-textPrimary">Cmd [1]</span>
                      <span className="opacity-50">|</span>
                      <span>PySpark Cell</span>
                    </div>
                    <div className="flex items-center gap-3">
                      <span className="flex items-center gap-1"><Clock className="w-3 h-3" /> Auto-compile</span>
                      <span className="opacity-50">|</span>
                      <span>128.0.0.1</span>
                    </div>
                  </div>

                  {/* Notebook prompt editor area */}
                  <div className="p-4 relative">
                    <textarea
                      value={prompt}
                      onChange={(e) => setPrompt(e.target.value)}
                      className="w-full min-h-[90px] bg-transparent text-sm border-0 focus:ring-0 resize-none font-mono text-textPrimary placeholder-textSecondary/50 outline-none leading-relaxed"
                      placeholder="Read customer data from ADLS, remove duplicates using latest timestamp, and load into a Delta table."
                    />

                    {/* Pre-fill suggestion quick chips */}
                    <div className="mt-4 pt-3 border-t border-card/40 flex flex-wrap gap-2 items-center">
                      <span className="text-[10px] font-bold text-textSecondary uppercase tracking-wider font-mono mr-1">Suggestions:</span>
                      {[
                        "Read Parquet with explicit schema",
                        "Optimize partitions using Z-Order",
                        "Join transactions with customer index",
                        "Cleanse NaN values"
                      ].map((chip) => (
                        <button
                          key={chip}
                          onClick={() => {
                            setPrompt(prev => prev + (prev.endsWith(" ") || prev === "" ? "" : " ") + chip);
                            triggerNotification("Prompt suggestion appended", "info");
                          }}
                          className="px-2.5 py-1 rounded-full bg-card hover:bg-card/80 border border-card/65 text-[10px] text-textSecondary hover:text-textPrimary transition"
                        >
                          + {chip}
                        </button>
                      ))}
                    </div>
                  </div>

                  {/* CTA compile/generate footer bar */}
                  <div className="px-4 py-3 bg-card/45 border-t border-card/60 flex flex-col sm:flex-row items-center justify-between gap-3">
                    <div className="flex items-center gap-2">
                      <span className="text-[10px] text-textSecondary font-mono">Input: {prompt.length} chars</span>
                    </div>
                    <button
                      onClick={handleGenerate}
                      disabled={isGenerating || !prompt.trim()}
                      className={`w-full sm:w-auto px-5 py-2 rounded text-xs font-bold flex items-center justify-center gap-2 bg-primary text-white hover:bg-primary/95 transition duration-200 shadow-lg glow-primary cursor-pointer disabled:opacity-40 disabled:pointer-events-none`}
                    >
                      <Play className="w-3.5 h-3.5 fill-white" />
                      <span>{isGenerating ? "Compiling PySpark..." : "Generate PySpark Code"}</span>
                    </button>
                  </div>
                </div>

                {/* 4. Generated Code Section */}
                <div className="rounded-lg border border-card bg-surface overflow-hidden shadow-xl flex flex-col">
                  
                  {/* Editor Top Toolbar */}
                  <div className="px-4 py-2 bg-card/65 border-b border-card flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <div className="w-2.5 h-2.5 rounded-full bg-success" />
                      <span className="font-mono text-xs font-bold text-textPrimary">{currentNotebook.name}</span>
                      <span className="px-1.5 py-0.5 rounded bg-success/10 border border-success/20 text-[9px] font-mono text-success">Delta ready</span>
                    </div>
                    
                    {/* Action buttons */}
                    <div className="flex items-center gap-1.5">
                      <button
                        onClick={handleCopyCode}
                        className="p-1.5 rounded hover:bg-card text-textSecondary hover:text-textPrimary transition title='Copy Code'"
                      >
                        {copied ? <Check className="w-3.5 h-3.5 text-success" /> : <Copy className="w-3.5 h-3.5" />}
                      </button>
                      
                      <button
                        onClick={handleDownloadCode}
                        className="p-1.5 rounded hover:bg-card text-textSecondary hover:text-textPrimary transition"
                        title="Download .py file"
                      >
                        <Download className="w-3.5 h-3.5" />
                      </button>

                      <div className="w-px h-4 bg-card/85 mx-1" />

                      <button
                        onClick={() => setIsFullScreen(!isFullScreen)}
                        className="p-1.5 rounded hover:bg-card text-textSecondary hover:text-textPrimary transition"
                        title={isFullScreen ? "Exit Fullscreen" : "Full Screen Focus Mode"}
                      >
                        {isFullScreen ? <Minimize2 className="w-3.5 h-3.5 text-primary" /> : <Maximize2 className="w-3.5 h-3.5" />}
                      </button>
                    </div>
                  </div>

                  {/* VS Code scrollable code area */}
                  <div className="bg-background/95 relative overflow-y-auto max-h-[450px] min-h-[300px] py-4 select-text flex flex-col font-mono">
                    
                    {/* Display animated streaming text */}
                    {renderHighlightedCode()}

                    {/* Blinking cursor typing line indicator */}
                    {isGenerating && (
                      <div className="px-4 flex items-center py-0.5 font-mono text-xs text-textSecondary/40 animate-pulse">
                        <span className="w-10 text-right pr-4 border-r border-card/60 select-none">···</span>
                        <span className="pl-4 italic flex items-center gap-2">
                          <RefreshCw className="w-3 h-3 animate-spin text-primary" /> Ingestion streaming pipeline compilation in progress...
                        </span>
                      </div>
                    )}

                    <div ref={editorEndRef} />
                  </div>

                  {/* Console Execution Log Footer */}
                  <div className="px-4 py-2.5 bg-card/30 border-t border-card/60 flex items-center justify-between font-mono text-[10px] text-textSecondary">
                    <div className="flex items-center gap-3">
                      <span className="flex items-center gap-1.5 text-success">
                        <span className="w-1.5 h-1.5 rounded-full bg-success" /> Code verified (0 errors, 0 warnings)
                      </span>
                    </div>
                    <div>
                      <span>UTF-8 • Python 3.10 • Spark 3.4.1</span>
                    </div>
                  </div>
                </div>

              </div>

              {/* 5. Right Sidebar (Pipeline Flow & AI Suggestions) */}
              {!isFullScreen && !rightSidebarCollapsed && (
                <aside className="w-full lg:w-80 bg-surface border-t lg:border-t-0 lg:border-l border-card flex flex-col">
                  
                  {/* Pipeline Flow Section */}
                  <div className="p-4 border-b border-card flex-1 flex flex-col overflow-y-auto">
                    <div className="flex items-center justify-between mb-4">
                      <h3 className="text-xs font-bold text-textPrimary uppercase tracking-widest font-mono">Pipeline Flow</h3>
                      <span className="px-1.5 py-0.5 rounded bg-primary/10 border border-primary/20 text-[9px] font-mono text-primary uppercase">Active</span>
                    </div>

                    {/* SVG Pipeline Graph */}
                    <div className="flex flex-col items-center py-4 bg-background/40 border border-card/60 rounded-md relative flex-1 min-h-[220px]">
                      
                      {currentNotebook.pipelineNodes.map((node, index) => {
                        const isActive = activePipelineNode === index;
                        const isCompleted = activePipelineNode > index;

                        return (
                          <React.Fragment key={node}>
                            {/* Connector line (not for the first element) */}
                            {index > 0 && (
                              <div className={`w-0.5 h-8 my-1 transition-all duration-500 ${
                                isCompleted ? "bg-success" : isActive ? "bg-primary" : "bg-card/75"
                              }`} />
                            )}

                            {/* Flow Node Card */}
                            <div className={`w-48 px-3 py-2.5 rounded-md border flex items-center gap-2.5 transition-all duration-300 ${
                              isActive 
                                ? "bg-card/90 border-primary shadow-lg scale-105" 
                                : isCompleted
                                  ? "bg-card/40 border-success/40 text-textSecondary"
                                  : "bg-card/20 border-card/85 text-textSecondary"
                            }`}>
                              <div className={`w-6 h-6 rounded flex items-center justify-center transition-colors ${
                                isActive 
                                  ? "bg-primary text-white" 
                                  : isCompleted 
                                    ? "bg-success/20 text-success" 
                                    : "bg-background text-textSecondary"
                              }`}>
                                {index === 0 && <Database className="w-3.5 h-3.5" />}
                                {index === 1 && <Cpu className="w-3.5 h-3.5" />}
                                {index === 2 && <Terminal className="w-3.5 h-3.5" />}
                                {index === 3 && <Layers className="w-3.5 h-3.5" />}
                              </div>
                              
                              <div className="flex flex-col text-left">
                                <span className={`text-[11px] font-bold ${isActive ? "text-textPrimary" : "text-textSecondary"}`}>{node}</span>
                                <span className="text-[9px] text-textSecondary font-mono uppercase tracking-wider">
                                  {isActive ? "Running..." : isCompleted ? "Success" : "Pending"}
                                </span>
                              </div>
                            </div>
                          </React.Fragment>
                        );
                      })}
                    </div>
                  </div>

                  {/* AI Suggestions Panel */}
                  <div className="p-4 border-t border-card bg-card/25 flex-1 flex flex-col overflow-y-auto">
                    <h3 className="text-xs font-bold text-textPrimary uppercase tracking-widest font-mono mb-3 flex items-center gap-1.5">
                      <Sparkles className="w-3.5 h-3.5 text-primary" /> AI Suggestions Panel
                    </h3>
                    
                    <div className="space-y-2.5">
                      {suggestions.map((item) => (
                        <div
                          key={item.id}
                          onClick={() => toggleSuggestion(item.id)}
                          className={`p-2.5 rounded border transition-all duration-200 cursor-pointer text-left select-none ${
                            item.active
                              ? "bg-card border-primary/50 text-textPrimary hover:bg-card/90"
                              : "bg-surface hover:bg-card/30 border-card/60 text-textSecondary"
                          }`}
                        >
                          <div className="flex items-center justify-between">
                            <span className="text-xs font-bold font-mono">{item.text}</span>
                            <span className={`w-3.5 h-3.5 rounded-full border flex items-center justify-center text-[8px] font-bold ${
                              item.active ? "bg-primary border-primary text-white" : "border-card/80 text-transparent"
                            }`}>
                              {item.active && <Check className="w-2.5 h-2.5 stroke-[3px]" />}
                            </span>
                          </div>
                          <p className="text-[10px] text-textSecondary mt-1 leading-relaxed">{item.desc}</p>
                        </div>
                      ))}
                    </div>
                  </div>

                </aside>
              )}
            </div>
          )}

          {/* Tab 2: Notebook Explorer */}
          {activeTab === "Notebook Explorer" && (
            <div className="flex-1 flex overflow-hidden bg-background">
              
              {/* Explorer sidebar (VS Code styling) */}
              <div className="w-64 border-r border-card bg-surface flex flex-col select-none">
                <div className="p-3 border-b border-card flex items-center justify-between">
                  <span className="text-xs font-bold text-textPrimary uppercase tracking-wider font-mono">Workspace Notebooks</span>
                  <span className="text-[10px] text-textSecondary font-mono">3 Files</span>
                </div>

                {/* Folder list */}
                <div className="p-2 flex-1 space-y-4">
                  <div>
                    {/* Folders */}
                    <div className="flex items-center gap-2 px-2 py-1 text-xs text-textPrimary font-semibold cursor-pointer">
                      <ChevronDown className="w-3.5 h-3.5 text-textSecondary" />
                      <Folder className="w-4 h-4 text-secondary fill-secondary/20" />
                      <span>pipelines/gold_analytics</span>
                    </div>

                    {/* Files list */}
                    <div className="ml-6 mt-1 space-y-1">
                      {NOTEBOOKS.map((nb) => {
                        const isCurrent = nb.id === selectedNotebookId;
                        return (
                          <div
                            key={nb.id}
                            onClick={() => {
                              setSelectedNotebookId(nb.id);
                              setActiveTab("Code Generator");
                            }}
                            className={`flex items-center justify-between px-2.5 py-1.5 rounded text-xs cursor-pointer transition ${
                              isCurrent 
                                ? "bg-card text-primary font-semibold border-l-2 border-primary" 
                                : "text-textSecondary hover:text-textPrimary hover:bg-card/35"
                            }`}
                          >
                            <div className="flex items-center gap-2 truncate">
                              <FileCode className="w-3.5 h-3.5 text-textSecondary" />
                              <span className="truncate">{nb.name}</span>
                            </div>
                            <span className="text-[9px] font-mono opacity-50">PySpark</span>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </div>
              </div>

              {/* Main explorer display panel */}
              <div className="flex-1 p-6 overflow-y-auto flex flex-col justify-center items-center text-center space-y-4">
                <div className="w-16 h-16 rounded-full bg-card flex items-center justify-center border border-card/85 text-textSecondary">
                  <Folder className="w-8 h-8" />
                </div>
                <div className="max-w-md">
                  <h3 className="text-lg font-bold text-textPrimary font-display">Databricks Notebook Explorer</h3>
                  <p className="text-sm text-textSecondary mt-2">
                    Click on any notebook file in the explorer sidebar to load it into the active code workspace. 
                    Your changes and configuration settings will be retained.
                  </p>
                  <button 
                    onClick={() => {
                      setSelectedNotebookId(NOTEBOOKS[0].id);
                      setActiveTab("Code Generator");
                    }}
                    className="mt-5 px-4 py-2 bg-card hover:bg-card/75 border border-card/80 rounded text-xs font-semibold text-primary transition"
                  >
                    Open Active Workspace
                  </button>
                </div>
              </div>

            </div>
          )}

          {/* Tab 3: Error Analyzer */}
          {activeTab === "Error Analyzer" && (
            <div className="flex-1 flex flex-col p-6 space-y-6 overflow-y-auto">
              
              <div className="space-y-1 text-left">
                <h2 className="text-2xl font-display font-extrabold tracking-tight text-textPrimary flex items-center gap-2">
                  Spark Error Analyzer
                  <Terminal className="w-5 h-5 text-primary" />
                </h2>
                <p className="text-sm text-textSecondary max-w-2xl leading-relaxed">
                  Paste traceback console logs from your Databricks cluster runs and get instant diagnostics, root cause analysis, and ready-to-run Spark DDL/DML fixes.
                </p>
              </div>

              {/* Traceback parser layout */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 items-start">
                
                {/* Input area */}
                <div className="bg-surface rounded-lg border border-card p-4 space-y-4 flex flex-col">
                  <div className="flex justify-between items-center">
                    <span className="text-xs font-bold font-mono text-textPrimary uppercase tracking-wider">Traceback console log</span>
                    <button 
                      onClick={() => setErrorInput(SAMPLE_SPARK_ERROR)}
                      className="px-2 py-1 rounded bg-card hover:bg-card/85 border border-card/80 text-[10px] text-textSecondary hover:text-textPrimary transition"
                    >
                      Load Sample Traceback
                    </button>
                  </div>

                  <textarea
                    value={errorInput}
                    onChange={(e) => setErrorInput(e.target.value)}
                    className="w-full min-h-[200px] bg-background text-xs text-textPrimary font-mono p-3 rounded border border-card/80 focus:border-primary/50 outline-none leading-relaxed resize-y"
                    placeholder="Paste your org.apache.spark.sql.AnalysisException traceback error message here..."
                  />

                  <button
                    onClick={handleAnalyzeError}
                    disabled={isAnalyzingError || !errorInput.trim()}
                    className="py-2.5 rounded bg-primary hover:bg-primary/95 text-xs font-bold text-white transition glow-primary flex items-center justify-center gap-2"
                  >
                    {isAnalyzingError ? (
                      <>
                        <RefreshCw className="w-4.5 h-4.5 animate-spin" /> Diagnosing logs...
                      </>
                    ) : (
                      "Analyze Stack Trace"
                    )}
                  </button>
                </div>

                {/* Output Analysis */}
                <div className="bg-surface rounded-lg border border-card p-4 space-y-4 text-left min-h-[345px] flex flex-col justify-between">
                  {errorDiagnosis ? (
                    <div className="space-y-4">
                      
                      {/* Diagnostic header banner */}
                      <div className="flex items-center justify-between pb-3 border-b border-card">
                        <div className="flex flex-col">
                          <span className="text-[10px] font-bold text-textSecondary uppercase tracking-wider font-mono">Detection Code</span>
                          <span className="text-xs font-bold text-primary font-mono mt-0.5">{errorDiagnosis.errorCode}</span>
                        </div>
                        <span className="px-2 py-1 rounded bg-red-950/30 border border-red-500/50 text-[9px] font-mono text-red-400 font-bold uppercase">
                          {errorDiagnosis.severity}
                        </span>
                      </div>

                      {/* Root cause */}
                      <div className="space-y-1">
                        <h4 className="text-xs font-bold text-textPrimary uppercase tracking-wider font-mono">Root Cause Diagnosis</h4>
                        <p className="text-xs text-textSecondary leading-relaxed">{errorDiagnosis.rootCause}</p>
                      </div>

                      {/* Fixes checklist */}
                      <div className="space-y-3 pt-2">
                        <h4 className="text-xs font-bold text-textPrimary uppercase tracking-wider font-mono">Recommended Actions</h4>
                        {errorDiagnosis.fixes.map((fix: any, index: number) => (
                          <div key={index} className="bg-background/80 border border-card rounded p-3 space-y-2">
                            <span className="text-[11px] font-bold text-success flex items-center gap-1.5">
                              <Check className="w-3.5 h-3.5" /> {fix.title}
                            </span>
                            <pre className="text-[10px] text-textSecondary font-mono bg-background p-2 rounded overflow-x-auto border border-card/65">
                              {fix.code}
                            </pre>
                          </div>
                        ))}
                      </div>

                    </div>
                  ) : (
                    <div className="flex-1 flex flex-col items-center justify-center text-center p-6 text-textSecondary">
                      <Terminal className="w-10 h-10 mb-3 opacity-40" />
                      <span className="text-xs font-bold font-mono">No Active Diagnosis Report</span>
                      <p className="text-[11px] max-w-xs mt-1">Paste a Databricks/PySpark log trace on the left and trigger diagnosis to inspect pipeline errors.</p>
                    </div>
                  )}

                  <div className="text-[9px] text-textSecondary font-mono opacity-50 pt-2 border-t border-card/45 mt-4">
                    Diagnostics engine powered by Unity Catalog Context analyzer.
                  </div>
                </div>

              </div>

            </div>
          )}

          {/* Tab 4: Documentation */}
          {activeTab === "Documentation" && (
            <div className="flex-1 p-6 space-y-6 overflow-y-auto text-left">
              <div className="space-y-1">
                <h2 className="text-2xl font-display font-extrabold tracking-tight text-textPrimary flex items-center gap-2">
                  PySpark & Delta API Reference
                  <BookOpen className="w-5 h-5 text-primary" />
                </h2>
                <p className="text-sm text-textSecondary max-w-2xl leading-relaxed">
                  Fast, indexed developer references for common PySpark commands, Delta tables optimization commands, and Window functions.
                </p>
              </div>

              {/* Doc Grid */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {[
                  {
                    title: "Window.partitionBy()",
                    category: "Analytical Functions",
                    syntax: "Window.partitionBy(*cols).orderBy(*cols)",
                    desc: "Groups data rows into partitions over which window aggregation is computed. Commonly used for running sums, moving averages, or ranking deduplications."
                  },
                  {
                    title: "df.write.format('delta')",
                    category: "Write Formats",
                    syntax: "df.write.format('delta').mode(saveMode).save(path)",
                    desc: "Saves the contents of the DataFrame as a Delta Lake table. Supports mergeSchema, overwriteSchema, and partitionBy options for transactional lakehouse storage."
                  },
                  {
                    title: "spark.readStream()",
                    category: "Structured Streaming",
                    syntax: "spark.readStream.format(source).load(path)",
                    desc: "Creates a streaming DataFrame that periodically ingests new files/events added to the source directory. Pairs with writeStream for near-real-time ingestion."
                  },
                  {
                    title: "OPTIMIZE table_name",
                    category: "Delta Management",
                    syntax: "spark.sql('OPTIMIZE table_name ZORDER BY (columns)')",
                    desc: "Coalesces small files in Delta tables to improve query speed. Applying Z-Order clustering on high-cardinality filter columns optimizes read runtimes."
                  },
                  {
                    title: "df.groupBy().agg()",
                    category: "Aggregations",
                    syntax: "df.groupBy(*cols).agg(*exprs)",
                    desc: "Aggregates grouped rows using expressions like sum(), count(), avg(), min(), or max(). Crucial for dimensional rollup stages in silver and gold layers."
                  },
                  {
                    title: "spark.sql()",
                    category: "Session Access",
                    syntax: "spark.sql(sqlQueryText)",
                    desc: "Executes an ANSI-compliant SQL query against registered metastore tables, returning the results as a standard PySpark DataFrame for mixed SQL/Python pipelines."
                  }
                ].map((doc) => (
                  <div key={doc.title} className="bg-surface rounded-lg border border-card p-4 space-y-3 hover:border-primary/40 transition">
                    <div className="flex items-center justify-between">
                      <span className="text-[10px] font-bold text-secondary uppercase font-mono tracking-wider">{doc.category}</span>
                      <ExternalLink className="w-3.5 h-3.5 text-textSecondary hover:text-textPrimary cursor-pointer" />
                    </div>
                    <h3 className="text-sm font-bold text-textPrimary font-mono">{doc.title}</h3>
                    <code className="block text-[10px] bg-background p-2 rounded border border-card/65 font-mono text-textSecondary">
                      {doc.syntax}
                    </code>
                    <p className="text-xs text-textSecondary leading-relaxed">{doc.desc}</p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Tab 5: Knowledge Search */}
          {activeTab === "Knowledge Search" && (
            <div className="flex-1 p-6 space-y-6 overflow-y-auto text-left">
              <div className="space-y-1">
                <h2 className="text-2xl font-display font-extrabold tracking-tight text-textPrimary flex items-center gap-2">
                  Knowledge Search
                  <Search className="w-5 h-5 text-primary" />
                </h2>
                <p className="text-sm text-textSecondary max-w-2xl leading-relaxed">
                  Search across collective enterprise knowledge bases, Lakehouse patterns, and best practices.
                </p>
              </div>

              {/* Search Box */}
              <div className="max-w-xl flex items-center gap-2 p-1.5 rounded-lg border border-card bg-surface">
                <Search className="w-4 h-4 text-textSecondary ml-2" />
                <input
                  type="text"
                  placeholder="Ask a question about Delta tables, sizing, clusters..."
                  className="bg-transparent border-0 outline-none text-xs text-textPrimary flex-1 focus:ring-0 p-1"
                />
                <button className="px-3 py-1.5 rounded bg-primary hover:bg-primary/95 text-[11px] font-bold text-white transition">
                  Search
                </button>
              </div>

              {/* Knowledge cards */}
              <div className="space-y-3 max-w-3xl">
                {[
                  {
                    title: "Z-Order vs Partitioning: When to use which?",
                    desc: "Partitioning is best suited for low-cardinality columns (e.g., date, country) where each partition holds at least 1 GB of data. For high-cardinality columns frequently used in WHERE filters (e.g., customer_id, product_sku), Z-Order clustering provides superior pruning without creating thousands of tiny files."
                  },
                  {
                    title: "How to prevent OOM errors on Spark joins?",
                    desc: "Out-Of-Memory (OOM) errors during join operations usually occur during shuffle stages when data distribution is skewed. Fixes include: (1) broadcasting the smaller DataFrame if it is < 100MB using broadcast(small_df), (2) enabling adaptive query execution (AQE) via spark.conf.set('spark.sql.adaptive.enabled', 'true'), or (3) introducing a salt column to resolve data skew."
                  },
                  {
                    title: "What is Delta Lake Liquid Clustering?",
                    desc: "Liquid Clustering replaces traditional partitioning and Z-Order, allowing you to define clustering keys dynamically. It scales automatically as table sizes grow, simplifies layout tuning, and is highly recommended for tables exceeding 1 TB in Databricks Runtime 13.3 LTS and above."
                  }
                ].map((item) => (
                  <div key={item.title} className="bg-surface rounded-lg border border-card p-4 space-y-2">
                    <h3 className="text-xs font-bold text-textPrimary font-mono">{item.title}</h3>
                    <p className="text-xs text-textSecondary leading-relaxed">{item.desc}</p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Tab 6: Settings */}
          {activeTab === "Settings" && (
            <div className="flex-1 p-6 space-y-6 overflow-y-auto text-left">
              <div className="space-y-1">
                <h2 className="text-2xl font-display font-extrabold tracking-tight text-textPrimary flex items-center gap-2">
                  System Settings
                  <Settings className="w-5 h-5 text-primary" />
                </h2>
                <p className="text-sm text-textSecondary max-w-2xl leading-relaxed">
                  Configure your Databricks Developer Copilot credentials, cluster sync triggers, and local workspace hooks.
                </p>
              </div>

              {/* Settings layout */}
              <div className="max-w-2xl space-y-6">
                
                {/* Configuration Card 1 */}
                <div className="bg-surface rounded-lg border border-card p-5 space-y-4">
                  <h3 className="text-xs font-bold text-textPrimary uppercase tracking-wider font-mono">Workspace Connection</h3>
                  
                  <div className="space-y-3">
                    <div>
                      <label className="block text-[11px] font-bold text-textSecondary uppercase font-mono mb-1">Databricks Workspace URL</label>
                      <input
                        type="text"
                        defaultValue="https://adb-179347590123.14.azuredatabricks.net"
                        className="w-full bg-background border border-card/85 p-2 rounded text-xs text-textPrimary outline-none focus:border-primary/50 font-mono"
                      />
                    </div>

                    <div>
                      <label className="block text-[11px] font-bold text-textSecondary uppercase font-mono mb-1">Personal Access Token (PAT)</label>
                      <input
                        type="password"
                        defaultValue="dapit*****************************"
                        className="w-full bg-background border border-card/85 p-2 rounded text-xs text-textPrimary outline-none focus:border-primary/50 font-mono"
                      />
                    </div>
                  </div>
                </div>

                {/* Configuration Card 2 */}
                <div className="bg-surface rounded-lg border border-card p-5 space-y-4">
                  <h3 className="text-xs font-bold text-textPrimary uppercase tracking-wider font-mono">Unity Catalog Defaults</h3>
                  
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                    <div>
                      <label className="block text-[11px] font-bold text-textSecondary uppercase font-mono mb-1">Default Catalog</label>
                      <input
                        type="text"
                        defaultValue="main"
                        className="w-full bg-background border border-card/85 p-2 rounded text-xs text-textPrimary outline-none focus:border-primary/50 font-mono"
                      />
                    </div>
                    <div>
                      <label className="block text-[11px] font-bold text-textSecondary uppercase font-mono mb-1">Default Schema</label>
                      <input
                        type="text"
                        defaultValue="silver"
                        className="w-full bg-background border border-card/85 p-2 rounded text-xs text-textPrimary outline-none focus:border-primary/50 font-mono"
                      />
                    </div>
                  </div>
                </div>

                <div className="flex gap-2">
                  <button 
                    onClick={() => triggerNotification("Settings saved successfully", "success")}
                    className="px-4 py-2 rounded bg-primary hover:bg-primary/95 text-xs font-bold text-white transition shadow-lg glow-primary cursor-pointer"
                  >
                    Save Changes
                  </button>
                  <button className="px-4 py-2 rounded bg-card hover:bg-card/75 border border-card/85 text-xs font-semibold text-textSecondary hover:text-textPrimary transition">
                    Test Connection
                  </button>
                </div>

              </div>
            </div>
          )}

        </main>
      </div>

      {/* Dynamic Toast Notifications */}
      {notification.show && (
        <div className={`fixed bottom-4 right-4 z-50 p-3 rounded-lg border shadow-xl flex items-center gap-2.5 max-w-sm transition-all duration-300 transform translate-y-0 ${
          notification.type === "success" 
            ? "bg-emerald-950/90 border-emerald-500/50 text-emerald-300 font-mono" 
            : "bg-card/90 border-primary/50 text-textPrimary font-mono"
        }`}>
          <div className={`w-2 h-2 rounded-full ${notification.type === "success" ? "bg-emerald-400" : "bg-primary"} animate-ping`} />
          <span className="text-xs">{notification.text}</span>
        </div>
      )}

      {/* 2. Top Header - Command Palette (VS Code style Modal) */}
      {isPaletteOpen && (
        <div className="fixed inset-0 bg-background/80 backdrop-blur-sm flex items-start justify-center pt-[15vh] z-50 px-4">
          <div className="w-full max-w-lg rounded-lg border border-card bg-surface overflow-hidden shadow-2xl flex flex-col">
            
            {/* Search Input */}
            <div className="flex items-center border-b border-card p-3 gap-2.5">
              <Search className="w-4.5 h-4.5 text-textSecondary" />
              <input
                type="text"
                autoFocus
                value={paletteSearch}
                onChange={(e) => setPaletteSearch(e.target.value)}
                placeholder="Type a file, command, or diagnostic task..."
                className="bg-transparent border-0 outline-none text-xs text-textPrimary flex-1 focus:ring-0"
              />
              <kbd className="px-1.5 py-0.5 rounded bg-card text-[9px] font-mono border border-card/50 select-none">ESC</kbd>
            </div>

            {/* Filtered items */}
            <div className="p-2 max-h-[300px] overflow-y-auto text-left font-mono">
              <div className="px-2 py-1.5 text-[9px] font-bold text-textSecondary uppercase tracking-widest">Commands & Actions</div>
              
              {[
                { name: "Generate PySpark Code", category: "Actions", tab: "Code Generator" },
                { name: "Run Diagnostics on stack traces", category: "Debugger", tab: "Error Analyzer" },
                { name: "List gold database tables", category: "Catalogs", tab: "Notebook Explorer" },
                { name: "Browse Window.partitionBy API reference", category: "Documentation", tab: "Documentation" },
                { name: "Optimize Delta Lake cluster storage", category: "Delta Management", tab: "Documentation" },
                { name: "Open Connection Settings", category: "System", tab: "Settings" }
              ]
                .filter(item => item.name.toLowerCase().includes(paletteSearch.toLowerCase()) || item.category.toLowerCase().includes(paletteSearch.toLowerCase()))
                .map((item, idx) => (
                  <div
                    key={idx}
                    onClick={() => {
                      setActiveTab(item.tab);
                      setIsPaletteOpen(false);
                      triggerNotification(`Navigated to: ${item.name}`, "info");
                    }}
                    className="flex items-center justify-between px-2.5 py-2 rounded hover:bg-card cursor-pointer transition text-xs text-textSecondary hover:text-textPrimary"
                  >
                    <div className="flex items-center gap-2">
                      <Terminal className="w-3.5 h-3.5 text-primary" />
                      <span>{item.name}</span>
                    </div>
                    <span className="text-[9px] uppercase px-1 rounded bg-card border border-card/75 text-textSecondary">{item.category}</span>
                  </div>
                ))}
            </div>

            {/* Palette Footer */}
            <div className="p-2 border-t border-card bg-card/25 flex justify-between items-center text-[9px] text-textSecondary font-mono px-3">
              <span>↑↓ to navigate • Enter to select</span>
              <span>Databricks Dev Copilot Command Bar</span>
            </div>
          </div>
        </div>
      )}

    </div>
  );
}
