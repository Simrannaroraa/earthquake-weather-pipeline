"""Generate the final standalone and embedded analytical dashboard HTML."""
import json
from pathlib import Path

def generate_dashboard():
    with open('/tmp/dashboard_data.json', 'r', encoding='utf-8') as f:
        events = json.load(f)

    json_str = json.dumps(events, ensure_ascii=False)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Earthquake & Weather Intelligence Dashboard | Track A Analytics</title>
  <script src="https://www.gstatic.com/antigravity/web/dev/tailwindcss.min.js"></script>
  <style>
    :root {{
      --bg: #0F172A;
      --card-bg: #1E293B;
      --card-border: #334155;
      --text-main: #F8FAFC;
      --text-muted: #94A3B8;
      --accent-blue: #38BDF8;
      --accent-red: #F87171;
      --accent-amber: #FBBF24;
      --accent-green: #34D399;
      --accent-purple: #A78BFA;
    }}
    body {{
      background-color: var(--bg);
      color: var(--text-main);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }}
    .metric-card {{
      background: linear-gradient(145deg, #1E293B, #0F172A);
      border: 1px solid var(--card-border);
      transition: transform 0.2s ease, border-color 0.2s ease;
    }}
    .metric-card:hover {{
      transform: translateY(-2px);
      border-color: var(--accent-blue);
    }}
    .chart-panel {{
      background-color: var(--card-bg);
      border: 1px solid var(--card-border);
    }}
    input[type=range] {{
      accent-color: var(--accent-blue);
    }}
    table tr:hover {{
      background-color: rgba(56, 189, 248, 0.08);
    }}
  </style>
</head>
<body class="p-4 md:p-8 min-h-screen">
  <div class="max-w-7xl mx-auto space-y-6">

    <!-- Header & Meta -->
    <header class="flex flex-col md:flex-row md:items-center justify-between gap-4 pb-6 border-b border-[var(--card-border)]">
      <div>
        <div class="flex items-center gap-2 mb-1">
          <span class="px-2.5 py-0.5 rounded-full text-xs font-semibold bg-sky-500/10 text-sky-400 border border-sky-500/30">
            Track A Capstone
          </span>
          <span class="px-2.5 py-0.5 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
            Gold Reporting Layer
          </span>
        </div>
        <h1 class="text-2xl md:text-3xl font-extrabold tracking-tight text-white">
          Earthquake & Weather Intelligence Dashboard
        </h1>
        <p class="text-sm text-[var(--text-muted)] mt-1">
          Analytical reporting over normalized 3NF & Gold Star Schema entities (USGS FDSN + Open-Meteo Current APIs).
        </p>
      </div>
      <div class="flex items-center gap-3 text-xs text-[var(--text-muted)] bg-slate-800/80 px-4 py-2.5 rounded-lg border border-slate-700">
        <div>
          <span class="block text-slate-400 font-medium">Dataset Status:</span>
          <span class="text-emerald-400 font-mono font-semibold">Live Validated Ingestion</span>
        </div>
        <div class="border-l border-slate-700 pl-3">
          <span class="block text-slate-400 font-medium">Events Analyzed:</span>
          <span class="text-white font-mono font-bold" id="header-count">117</span>
        </div>
      </div>
    </header>

    <!-- KPI Summary Row -->
    <section class="grid grid-cols-2 md:grid-cols-5 gap-4">
      <div class="metric-card p-4 rounded-xl">
        <p class="text-xs uppercase tracking-wider text-[var(--text-muted)] font-semibold">Total Seismic Events</p>
        <p class="text-2xl md:text-3xl font-black text-white mt-1" id="kpi-total">117</p>
        <p class="text-xs text-sky-400 mt-1">Past 24h & Historical Capture</p>
      </div>
      <div class="metric-card p-4 rounded-xl">
        <p class="text-xs uppercase tracking-wider text-[var(--text-muted)] font-semibold">Max Magnitude</p>
        <p class="text-2xl md:text-3xl font-black text-red-400 mt-1" id="kpi-max-mag">5.5 M</p>
        <p class="text-xs text-[var(--text-muted)] mt-1">Hamakita & Hasaki, Japan</p>
      </div>
      <div class="metric-card p-4 rounded-xl">
        <p class="text-xs uppercase tracking-wider text-[var(--text-muted)] font-semibold">Weather Enrichment</p>
        <p class="text-2xl md:text-3xl font-black text-emerald-400 mt-1" id="kpi-weather-pct">65.0%</p>
        <p class="text-xs text-[var(--text-muted)] mt-1"><span id="kpi-weather-count">76</span> / 117 Snapshots Matched</p>
      </div>
      <div class="metric-card p-4 rounded-xl">
        <p class="text-xs uppercase tracking-wider text-[var(--text-muted)] font-semibold">Average Focal Depth</p>
        <p class="text-2xl md:text-3xl font-black text-amber-400 mt-1" id="kpi-avg-depth">47.8 km</p>
        <p class="text-xs text-[var(--text-muted)] mt-1">Crustal to Subduction Range</p>
      </div>
      <div class="metric-card p-4 rounded-xl col-span-2 md:col-span-1">
        <p class="text-xs uppercase tracking-wider text-[var(--text-muted)] font-semibold">Surface Air Temp (Avg)</p>
        <p class="text-2xl md:text-3xl font-black text-cyan-400 mt-1" id="kpi-avg-temp">14.0°C</p>
        <p class="text-xs text-[var(--text-muted)] mt-1">Range: -9.8°C to +29.1°C</p>
      </div>
    </section>

    <!-- Interactive Filter Toolbar -->
    <section class="chart-panel p-4 rounded-xl space-y-3">
      <div class="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <!-- Search -->
        <div class="flex-1 relative">
          <input type="text" id="filter-search" placeholder="Search by region, place, or event ID..."
                 class="w-full bg-slate-900 border border-slate-700 rounded-lg px-3.5 py-2 text-sm text-white placeholder-slate-500 focus:outline-none focus:border-sky-500">
        </div>

        <!-- Magnitude Slider -->
        <div class="flex items-center gap-3">
          <span class="text-xs font-semibold text-[var(--text-muted)] whitespace-nowrap">Min Magnitude:</span>
          <input type="range" id="filter-mag" min="2.5" max="5.5" step="0.1" value="2.5" class="w-28 md:w-36">
          <span class="text-sm font-mono font-bold text-sky-400 w-10 text-right" id="filter-mag-val">≥ 2.5</span>
        </div>

        <!-- Weather Condition -->
        <div class="flex items-center gap-2">
          <span class="text-xs font-semibold text-[var(--text-muted)] whitespace-nowrap">Weather:</span>
          <select id="filter-weather" class="bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm text-white focus:outline-none focus:border-sky-500">
            <option value="ALL">All Snapshots (117)</option>
            <option value="WITH_WEATHER">Enriched with Weather (76)</option>
            <option value="CLEAR">Clear Sky / Mainly Clear (34)</option>
            <option value="CLOUDY">Overcast / Cloudy (30)</option>
            <option value="RAIN">Rain / Drizzle (8)</option>
            <option value="SNOW_FOG">Snow / Fog (4)</option>
          </select>
        </div>

        <!-- Reset Button -->
        <button id="btn-reset" class="px-3 py-2 text-xs font-semibold bg-slate-800 hover:bg-slate-700 border border-slate-600 rounded-lg text-slate-300 transition">
          Reset Filters
        </button>
      </div>
      <div class="flex items-center justify-between text-xs text-[var(--text-muted)] pt-1 border-t border-slate-800">
        <span>Displaying <strong class="text-white" id="filter-status-count">117</strong> of 117 records</span>
        <span class="text-amber-400/90 italic">Note: Weather fields represent current/forecast grid snapshots at ingestion time.</span>
      </div>
    </section>

    <!-- Visual Analytics: Map & Scatter -->
    <section class="grid grid-cols-1 lg:grid-cols-3 gap-6">
      
      <!-- Global Map Panel -->
      <div class="chart-panel p-5 rounded-xl lg:col-span-2 flex flex-col">
        <div class="flex items-center justify-between mb-3">
          <div>
            <h2 class="text-base font-bold text-white flex items-center gap-2">
              <span>Global Seismic Epicenters Map</span>
              <span class="text-xs font-normal text-slate-400">(WGS 84 Equirectangular Projection)</span>
            </h2>
            <p class="text-xs text-[var(--text-muted)]">Circle size scales with magnitude; color denotes seismic intensity scale.</p>
          </div>
          <div class="flex items-center gap-3 text-xs">
            <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-full bg-red-400"></span> ≥ 5.0 M</span>
            <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-full bg-amber-400"></span> 4.0 - 4.9 M</span>
            <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-full bg-cyan-400"></span> 3.0 - 3.9 M</span>
            <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded-full bg-emerald-400"></span> < 3.0 M</span>
          </div>
        </div>
        <div class="relative flex-1 w-full bg-slate-950/60 rounded-lg border border-slate-800 overflow-hidden" style="min-height: 340px;">
          <canvas id="mapCanvas" class="w-full h-full block cursor-crosshair"></canvas>
          <div id="mapTooltip" class="absolute hidden bg-slate-900/95 border border-slate-700 text-xs text-white p-2.5 rounded-lg shadow-xl pointer-events-none z-10 space-y-1"></div>
        </div>
      </div>

      <!-- Depth vs Magnitude Scatter -->
      <div class="chart-panel p-5 rounded-xl flex flex-col">
        <div class="mb-3">
          <h2 class="text-base font-bold text-white">Focal Depth vs Magnitude</h2>
          <p class="text-xs text-[var(--text-muted)]">Shallow crustal ruptures (&lt;35km) vs deep subduction zone events (&gt;70km).</p>
        </div>
        <div class="relative flex-1 w-full bg-slate-950/60 rounded-lg border border-slate-800 overflow-hidden" style="min-height: 340px;">
          <canvas id="scatterCanvas" class="w-full h-full block"></canvas>
        </div>
      </div>

    </section>

    <!-- Visual Analytics: Magnitude Buckets & Weather Breakdown -->
    <section class="grid grid-cols-1 md:grid-cols-2 gap-6">

      <!-- Magnitude Distribution -->
      <div class="chart-panel p-5 rounded-xl">
        <div class="mb-4">
          <h2 class="text-base font-bold text-white">Magnitude Category Breakdown</h2>
          <p class="text-xs text-[var(--text-muted)]">Event counts by Richter / Moment Magnitude severity classes.</p>
        </div>
        <div class="space-y-3" id="mag-bucket-bars">
          <!-- Filled by JS -->
        </div>
      </div>

      <!-- Weather Conditions at Ingestion -->
      <div class="chart-panel p-5 rounded-xl">
        <div class="mb-4">
          <h2 class="text-base font-bold text-white">Atmospheric Conditions at Epicenter</h2>
          <p class="text-xs text-[var(--text-muted)]">Distribution of Open-Meteo WMO weather codes captured at ingestion time.</p>
        </div>
        <div class="space-y-3" id="weather-code-bars">
          <!-- Filled by JS -->
        </div>
      </div>

    </section>

    <!-- Executive Analytical Insights Panel -->
    <section class="chart-panel p-6 rounded-xl border-l-4 border-l-sky-500">
      <h2 class="text-lg font-black text-white flex items-center gap-2">
        <svg class="w-5 h-5 text-sky-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z"/></svg>
        Executive Analytical Insights & Seismological Takeaways
      </h2>
      <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4 text-xs leading-relaxed text-slate-300">
        <div class="bg-slate-900/60 p-3.5 rounded-lg border border-slate-800 space-y-1">
          <h3 class="font-bold text-sky-400 text-sm">1. Pacific Ring of Fire Dominance</h3>
          <p>
            100% of major earthquakes ($\ge 5.0$ M) cluster along circum-Pacific tectonic boundaries: Japan (Hamakita & Hasaki at 5.5 Mww), Indonesia (Pulau Pulau Tanimbar at 5.4 Mb), Papua New Guinea (5.1 Mww), and the Solomon Islands (5.2 Mww). Intraplate continental regions (e.g. Utah, Puerto Rico) experience frequent swarm activity but lower magnitudes ($<3.5$ M).
          </p>
        </div>
        <div class="bg-slate-900/60 p-3.5 rounded-lg border border-slate-800 space-y-1">
          <h3 class="font-bold text-amber-400 text-sm">2. Focal Depth Bimodality</h3>
          <p>
            Earthquakes stratify into two distinct mechanical regimes: shallow crustal events (depth $\le 20$ km, accounting for 62% of records) which carry high surface shaking risk, versus deep oceanic subduction slab events (depth $70 - 140$ km, e.g. south of Fiji at 136 km) where seismic waves attenuate significantly before reaching surface populations.
          </p>
        </div>
        <div class="bg-slate-900/60 p-3.5 rounded-lg border border-slate-800 space-y-1">
          <h3 class="font-bold text-emerald-400 text-sm">3. Meteorological Context & Environmental Variance</h3>
          <p>
            The 76 weather snapshots capture extreme climatic diversity across active epicenters: surface temperatures span from -9.8&deg;C in cold high-latitude regions (e.g., Fox Islands, Alaska) to +29.1&deg;C across equatorial maritime zones (Indonesia). Clear to overcast skies account for 84.2% of captured weather observations.
          </p>
        </div>
        <div class="bg-slate-900/60 p-3.5 rounded-lg border border-slate-800 space-y-1">
          <h3 class="font-bold text-purple-400 text-sm">4. Data Pipeline Architecture & Temporal Decoupling</h3>
          <p>
            The pipeline maintains strict analytical integrity: weather conditions are stored with distinct `requested_at_utc` and `observation_time_utc` timestamps, explicitly documenting that atmospheric conditions represent the surface state at ingestion time, rather than subterranean pressure conditions at the exact moment of tectonic rupture.
          </p>
        </div>
      </div>
    </section>

    <!-- Event Drill-Down Data Table -->
    <section class="chart-panel rounded-xl overflow-hidden">
      <div class="p-4 border-b border-[var(--card-border)] flex items-center justify-between">
        <div>
          <h2 class="text-base font-bold text-white">Event Detail Drill-Down (Normalized 3NF Entities)</h2>
          <p class="text-xs text-[var(--text-muted)]">Click on any USGS link to view official seismological network details.</p>
        </div>
        <span class="text-xs text-slate-400 font-mono" id="table-record-indicator">Showing records</span>
      </div>
      <div class="overflow-x-auto max-h-96">
        <table class="w-full text-left text-xs whitespace-nowrap">
          <thead class="bg-slate-900 text-slate-400 uppercase font-semibold sticky top-0 border-b border-slate-800">
            <tr>
              <th class="p-3">Event ID</th>
              <th class="p-3">Time (UTC)</th>
              <th class="p-3">Magnitude</th>
              <th class="p-3">Place / Region</th>
              <th class="p-3">Depth (km)</th>
              <th class="p-3">Temp (°C)</th>
              <th class="p-3">Wind (km/h)</th>
              <th class="p-3">Weather</th>
              <th class="p-3">Status</th>
              <th class="p-3">Source</th>
            </tr>
          </thead>
          <tbody id="events-table-body" class="divide-y divide-slate-800/60 font-mono">
            <!-- Populated by JS -->
          </tbody>
        </table>
      </div>
    </section>

    <!-- Footer -->
    <footer class="pt-6 pb-8 border-t border-[var(--card-border)] text-xs text-center text-slate-500 space-y-1">
      <p>Data Sources: United States Geological Survey (USGS FDSN Web Services) & Open-Meteo Historical / Current Weather API.</p>
      <p>Pipeline Architecture: Python Ingestion &rarr; Bronze Delta Landing &rarr; Silver 3NF Normalized Schema &rarr; Gold Star Schema Analytics.</p>
    </footer>

  </div>

  <!-- Client-side Interactive Logic -->
  <script>
    const RAW_DATA = {json_str};

    let filteredData = [...RAW_DATA];

    const weatherCodeMap = {{
      0: "Clear sky",
      1: "Mainly clear",
      2: "Partly cloudy",
      3: "Overcast",
      45: "Fog",
      51: "Light drizzle",
      53: "Moderate drizzle",
      61: "Slight rain",
      63: "Moderate rain",
      80: "Rain showers",
      85: "Snow showers",
      95: "Thunderstorm"
    }};

    function getWeatherCategory(code) {{
      if (code === null || code === undefined) return "NONE";
      if (code === 0 || code === 1) return "CLEAR";
      if (code === 2 || code === 3) return "CLOUDY";
      if (code >= 50 && code <= 80) return "RAIN";
      if (code === 45 || code === 85 || code === 95) return "SNOW_FOG";
      return "OTHER";
    }}

    function applyFilters() {{
      const query = document.getElementById("filter-search").value.toLowerCase().trim();
      const minMag = parseFloat(document.getElementById("filter-mag").value);
      const weatherCategory = document.getElementById("filter-weather").value;

      document.getElementById("filter-mag-val").innerText = `≥ ${{minMag.toFixed(1)}}`;

      filteredData = RAW_DATA.filter(item => {{
        if (item.magnitude < minMag) return false;

        if (query) {{
          const placeMatch = (item.place || "").toLowerCase().includes(query);
          const idMatch = (item.event_id || "").toLowerCase().includes(query);
          if (!placeMatch && !idMatch) return false;
        }}

        if (weatherCategory === "WITH_WEATHER") {{
          if (item.temperature_c === null) return false;
        }} else if (weatherCategory === "CLEAR") {{
          if (getWeatherCategory(item.weather_code) !== "CLEAR") return false;
        }} else if (weatherCategory === "CLOUDY") {{
          if (getWeatherCategory(item.weather_code) !== "CLOUDY") return false;
        }} else if (weatherCategory === "RAIN") {{
          if (getWeatherCategory(item.weather_code) !== "RAIN") return false;
        }} else if (weatherCategory === "SNOW_FOG") {{
          if (getWeatherCategory(item.weather_code) !== "SNOW_FOG") return false;
        }}

        return true;
      }});

      updateKPIs();
      renderMap();
      renderScatter();
      renderMagnitudeBuckets();
      renderWeatherDistribution();
      renderTable();
    }}

    function updateKPIs() {{
      const count = filteredData.length;
      document.getElementById("header-count").innerText = count;
      document.getElementById("filter-status-count").innerText = count;
      document.getElementById("kpi-total").innerText = count;

      if (count === 0) {{
        document.getElementById("kpi-max-mag").innerText = "N/A";
        document.getElementById("kpi-weather-pct").innerText = "0%";
        document.getElementById("kpi-weather-count").innerText = "0";
        document.getElementById("kpi-avg-depth").innerText = "N/A";
        document.getElementById("kpi-avg-temp").innerText = "N/A";
        return;
      }}

      let maxM = 0;
      let depthSum = 0;
      let weatherCount = 0;
      let tempSum = 0;

      filteredData.forEach(d => {{
        if (d.magnitude > maxM) maxM = d.magnitude;
        if (d.depth_km !== null) depthSum += d.depth_km;
        if (d.temperature_c !== null) {{
          weatherCount++;
          tempSum += d.temperature_c;
        }}
      }});

      document.getElementById("kpi-max-mag").innerText = `${{maxM.toFixed(1)}} M`;
      document.getElementById("kpi-weather-pct").innerText = `${{((weatherCount / count) * 100).toFixed(1)}}%`;
      document.getElementById("kpi-weather-count").innerText = weatherCount;
      document.getElementById("kpi-avg-depth").innerText = `${{(depthSum / count).toFixed(1)}} km`;
      document.getElementById("kpi-avg-temp").innerText = weatherCount > 0 ? `${{(tempSum / weatherCount).toFixed(1)}}°C` : "N/A";
    }}

    function renderMap() {{
      const canvas = document.getElementById("mapCanvas");
      const ctx = canvas.getContext("2d");
      const rect = canvas.getBoundingClientRect();
      const w = canvas.width = rect.width * 2;
      const h = canvas.height = rect.height * 2;

      ctx.clearRect(0, 0, w, h);

      // Draw Grid Lines
      ctx.strokeStyle = "rgba(51, 65, 85, 0.4)";
      ctx.lineWidth = 1;
      for (let lon = -180; lon <= 180; lon += 60) {{
        const x = ((lon + 180) / 360) * w;
        ctx.beginPath();
        ctx.moveTo(x, 0);
        ctx.lineTo(x, h);
        ctx.stroke();
      }}
      for (let lat = -90; lat <= 90; lat += 30) {{
        const y = ((90 - lat) / 180) * h;
        ctx.beginPath();
        ctx.moveTo(0, y);
        ctx.lineTo(w, y);
        ctx.stroke();
      }}

      // Plot Earthquakes
      filteredData.forEach(d => {{
        const x = ((d.longitude + 180) / 360) * w;
        const y = ((90 - d.latitude) / 180) * h;
        const radius = Math.max(3, (d.magnitude - 1.5) * 4);

        ctx.beginPath();
        ctx.arc(x, y, radius, 0, Math.PI * 2);

        let color = "rgba(52, 211, 153, 0.75)";
        if (d.magnitude >= 5.0) color = "rgba(248, 113, 113, 0.85)";
        else if (d.magnitude >= 4.0) color = "rgba(251, 191, 36, 0.8)";
        else if (d.magnitude >= 3.0) color = "rgba(56, 189, 248, 0.75)";

        ctx.fillStyle = color;
        ctx.fill();
        ctx.strokeStyle = "rgba(255, 255, 255, 0.6)";
        ctx.lineWidth = 1;
        ctx.stroke();
      }});
    }}

    function renderScatter() {{
      const canvas = document.getElementById("scatterCanvas");
      const ctx = canvas.getContext("2d");
      const rect = canvas.getBoundingClientRect();
      const w = canvas.width = rect.width * 2;
      const h = canvas.height = rect.height * 2;

      ctx.clearRect(0, 0, w, h);

      const pad = 40;
      const plotW = w - pad * 2;
      const plotH = h - pad * 2;

      // Axes
      ctx.strokeStyle = "#475569";
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(pad, pad);
      ctx.lineTo(pad, h - pad);
      ctx.lineTo(w - pad, h - pad);
      ctx.stroke();

      // Axis labels
      ctx.fillStyle = "#94A3B8";
      ctx.font = "20px monospace";
      ctx.fillText("Depth: 0km", pad + 10, pad + 20);
      ctx.fillText("150km+", pad + 10, h - pad - 10);
      ctx.fillText("Mag 2.5", pad, h - pad + 30);
      ctx.fillText("Mag 6.0", w - pad - 80, h - pad + 30);

      // Plot Points
      filteredData.forEach(d => {{
        const magNorm = Math.min(1, Math.max(0, (d.magnitude - 2.5) / 3.5));
        const depthNorm = Math.min(1, Math.max(0, (d.depth_km || 10) / 150));

        const x = pad + magNorm * plotW;
        const y = pad + depthNorm * plotH;

        ctx.beginPath();
        ctx.arc(x, y, 6, 0, Math.PI * 2);
        ctx.fillStyle = d.magnitude >= 5.0 ? "#F87171" : (d.magnitude >= 4.0 ? "#FBBF24" : "#38BDF8");
        ctx.fill();
      }});
    }}

    function renderMagnitudeBuckets() {{
      const container = document.getElementById("mag-bucket-bars");
      container.innerHTML = "";

      const buckets = [
        {{ label: "2.5 - 2.9 (Minor)", count: 0, color: "bg-emerald-500" }},
        {{ label: "3.0 - 3.9 (Light)", count: 0, color: "bg-cyan-500" }},
        {{ label: "4.0 - 4.9 (Moderate)", count: 0, color: "bg-amber-500" }},
        {{ label: "5.0+ (Strong)", count: 0, color: "bg-red-500" }},
      ];

      filteredData.forEach(d => {{
        if (d.magnitude < 3.0) buckets[0].count++;
        else if (d.magnitude < 4.0) buckets[1].count++;
        else if (d.magnitude < 5.0) buckets[2].count++;
        else buckets[3].count++;
      }});

      const total = filteredData.length || 1;

      buckets.forEach(b => {{
        const pct = ((b.count / total) * 100).toFixed(1);
        container.innerHTML += `
          <div class="space-y-1">
            <div class="flex justify-between text-xs">
              <span class="font-medium text-slate-300">${{b.label}}</span>
              <span class="font-mono text-white">${{b.count}} <span class="text-slate-500">(${{pct}}%)</span></span>
            </div>
            <div class="h-2 w-full bg-slate-900 rounded-full overflow-hidden">
              <div class="${{b.color}} h-full transition-all duration-300" style="width: ${{pct}}%"></div>
            </div>
          </div>
        `;
      }});
    }}

    function renderWeatherDistribution() {{
      const container = document.getElementById("weather-code-bars");
      container.innerHTML = "";

      const counts = {{}};
      let totalEnriched = 0;

      filteredData.forEach(d => {{
        if (d.weather_code !== null) {{
          counts[d.weather_code] = (counts[d.weather_code] || 0) + 1;
          totalEnriched++;
        }}
      }});

      if (totalEnriched === 0) {{
        container.innerHTML = "<p class='text-xs text-slate-500 italic'>No weather snapshots match current filters.</p>";
        return;
      }}

      const sortedCodes = Object.entries(counts).sort((a,b) => b[1] - a[1]).slice(0, 4);

      sortedCodes.forEach(([code, count]) => {{
        const label = weatherCodeMap[code] || `WMO Code ${{code}}`;
        const pct = ((count / totalEnriched) * 100).toFixed(1);
        container.innerHTML += `
          <div class="space-y-1">
            <div class="flex justify-between text-xs">
              <span class="font-medium text-slate-300">${{label}}</span>
              <span class="font-mono text-cyan-400">${{count}} <span class="text-slate-500">(${{pct}}%)</span></span>
            </div>
            <div class="h-2 w-full bg-slate-900 rounded-full overflow-hidden">
              <div class="bg-cyan-500 h-full transition-all duration-300" style="width: ${{pct}}%"></div>
            </div>
          </div>
        `;
      }});
    }}

    function renderTable() {{
      const tbody = document.getElementById("events-table-body");
      tbody.innerHTML = "";

      const slice = filteredData.slice(0, 30);
      document.getElementById("table-record-indicator").innerText = `Showing top ${{slice.length}} of ${{filteredData.length}} records`;

      slice.forEach(d => {{
        const timeShort = (d.event_time_utc || "").replace("T", " ").slice(0, 19);
        const tempStr = d.temperature_c !== null ? `${{d.temperature_c.toFixed(1)}}°C` : "<span class='text-slate-600'>N/A</span>";
        const windStr = d.wind_speed_kmh !== null ? `${{d.wind_speed_kmh.toFixed(1)}}` : "-";
        const weatherDesc = d.weather_code !== null ? (weatherCodeMap[d.weather_code] || `Code ${{d.weather_code}}`) : "<span class='text-slate-600'>None</span>";

        let magBadge = "text-emerald-400 font-bold";
        if (d.magnitude >= 5.0) magBadge = "text-red-400 font-black";
        else if (d.magnitude >= 4.0) magBadge = "text-amber-400 font-bold";
        else if (d.magnitude >= 3.0) magBadge = "text-cyan-400 font-bold";

        tbody.innerHTML += `
          <tr class="transition-colors">
            <td class="p-3 font-semibold text-slate-300">${{d.event_id}}</td>
            <td class="p-3 text-slate-400">${{timeShort}}</td>
            <td class="p-3 ${{magBadge}}">${{d.magnitude.toFixed(1)}} <span class="text-xs font-normal text-slate-500">${{d.mag_type}}</span></td>
            <td class="p-3 text-slate-200 font-sans truncate max-w-xs" title="${{d.place}}">${{d.place}}</td>
            <td class="p-3 text-slate-400">${{d.depth_km !== null ? d.depth_km.toFixed(1) : '-'}}</td>
            <td class="p-3 text-cyan-300 font-bold">${{tempStr}}</td>
            <td class="p-3 text-slate-400">${{windStr}}</td>
            <td class="p-3 text-slate-300 font-sans">${{weatherDesc}}</td>
            <td class="p-3 text-slate-400 capitalize">${{d.status || '-'}}</td>
            <td class="p-3"><a href="${{d.source_url}}" target="_blank" class="text-sky-400 hover:text-sky-300 underline font-sans">USGS &nearr;</a></td>
          </tr>
        `;
      }});
    }}

    // Event Listeners
    document.getElementById("filter-search").addEventListener("input", applyFilters);
    document.getElementById("filter-mag").addEventListener("input", applyFilters);
    document.getElementById("filter-weather").addEventListener("change", applyFilters);
    document.getElementById("btn-reset").addEventListener("click", () => {{
      document.getElementById("filter-search").value = "";
      document.getElementById("filter-mag").value = "2.5";
      document.getElementById("filter-weather").value = "ALL";
      applyFilters();
    }});

    window.addEventListener("resize", () => {{
      renderMap();
      renderScatter();
    }});

    // Initial Render
    applyFilters();
  </script>
</body>
</html>
"""
    out_path = Path("docs/analytical_dashboard.html")
    out_path.write_text(html_content, encoding="utf-8")
    print(f"Generated {out_path} ({len(html_content)} bytes)")
    return html_content

if __name__ == "__main__":
    generate_dashboard()
