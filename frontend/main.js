const API_BASE = 'http://localhost:3000';
let lastRows = [];

async function fetchTrips(limit = 200, offset = 0, start = null, end = null) {
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) });
  if (start) params.set('start', start);
  if (end) params.set('end', end);
  const res = await fetch(`${API_BASE}/api/trips?${params.toString()}`);
  if (!res.ok) throw new Error(`Failed to fetch trips: ${res.status}`);
  return res.json(); // { data, pagination }
}

function renderBusiestHoursTop5(rows) {
  const ctx = document.getElementById('busiestHoursChart')?.getContext('2d');
  if (!ctx) return;
  const counts = Array.from({ length: 24 }, () => 0);
  rows.forEach((t) => {
    const d = new Date(t.pickup_datetime);
    if (!isNaN(d)) counts[d.getHours()] += 1;
  });
  const pairs = counts.map((c, h) => ({ h, c }));
  pairs.sort((a, b) => b.c - a.c);
  const top5 = pairs.slice(0, 5).sort((a, b) => a.h - b.h);
  const labels = top5.map(p => String(p.h));
  const data = top5.map(p => p.c);
  if (busiestHoursChart) busiestHoursChart.destroy();
  busiestHoursChart = new Chart(ctx, {
    type: 'bar',
    data: { labels, datasets: [{ label: 'Trips', data, backgroundColor: '#10b981' }] },
    options: { responsive: true, scales: { y: { beginAtZero: true } } }
  });
}

function renderFareTrendDaily(rows) {
  const ctx = document.getElementById('fareTrendChart')?.getContext('2d');
  if (!ctx) return;
  const map = new Map(); // date -> {sum, n}
  rows.forEach((t) => {
    const d = new Date(t.pickup_datetime);
    const farePerKm = t.fare_per_km != null ? Number(t.fare_per_km) : NaN;
    if (isNaN(farePerKm) || !isFinite(farePerKm) || isNaN(d)) return;
    const key = d.toISOString().slice(0, 10);
    const v = map.get(key) || { sum: 0, n: 0 };
    v.sum += farePerKm; v.n += 1; map.set(key, v);
  });
  const dates = Array.from(map.keys()).sort();
  const avgs = dates.map((k) => map.get(k).sum / map.get(k).n);
  if (fareTrendChart) fareTrendChart.destroy();
  fareTrendChart = new Chart(ctx, {
    type: 'line',
    data: { labels: dates, datasets: [{ label: 'Avg Fare per km ($)', data: avgs, borderColor: '#ef4444', backgroundColor: 'rgba(239,68,68,0.2)', fill: true }] },
    options: { responsive: true, plugins: { legend: { display: true } }, scales: { y: { beginAtZero: true } } }
  });
}

async function fetchHeatmapManual(precision = 3, limitRows = 50000, k = 10000) {
  const params = new URLSearchParams({ precision: String(precision), limitRows: String(limitRows), k: String(k) });
  const res = await fetch(`${API_BASE}/api/heatmap-manual?${params.toString()}`);
  if (!res.ok) throw new Error(`Failed to fetch heatmap: ${res.status}`);
  return res.json(); // { precision, sampled, k, data: [{lat, lon, count}] }
}

async function fetchTopRoutesManual(precision = 3, limitRows = 50000, k = 10) {
  const params = new URLSearchParams({ precision: String(precision), limitRows: String(limitRows), k: String(k) });
  const res = await fetch(`${API_BASE}/api/top-routes-manual?${params.toString()}`);
  if (!res.ok) throw new Error(`Failed to fetch top routes: ${res.status}`);
  return res.json(); // { precision, sampled, k, data: [{pickup_lat, pickup_lon, dropoff_lat, dropoff_lon, count, ...}] }
}

function updateTable(rows) {
  const tbody = document.getElementById('trips-tbody');
  if (!tbody) return;
  tbody.innerHTML = '';
  rows.forEach((t) => {
    const dSec = t.trip_duration != null ? Number(t.trip_duration) : null;
    const durationMin = dSec != null && isFinite(dSec) ? dSec / 60 : null;
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${t.id ?? ''}</td>
      <td>${t.pickup_datetime ?? ''}</td>
      <td>${t.dropoff_datetime ?? ''}</td>
      <td>${durationMin != null ? Number(durationMin).toFixed(1) : '—'}</td>
      <td>${t.trip_distance != null ? Number(t.trip_distance).toFixed(2) : '—'}</td>
      <td>${t.trip_speed_kmh != null ? Number(t.trip_speed_kmh).toFixed(1) : '—'}</td>
      <td>${t.passenger_count ?? '—'}</td>
    `;
    tbody.appendChild(tr);
  });
}

let durationChart, distanceChart, speedChart, hourChart, heatmapChart, topRoutesChart, busiestHoursChart, fareTrendChart;
function renderCharts(rows) {
  const ctxDuration = document.getElementById('durationChart')?.getContext('2d');
  const ctxDistance = document.getElementById('distanceChart')?.getContext('2d');
  const ctxSpeed = document.getElementById('speedChart')?.getContext('2d');
  const ctxHour = document.getElementById('hourChart')?.getContext('2d');
  if (!ctxDuration || !ctxDistance || !ctxSpeed || !ctxHour) return;

  // Duration histogram (minutes)
  const durBins = [0, 5, 10, 15, 20, 30, 45, 60];
  const durLabels = ['<5', '5-10', '10-15', '15-20', '20-30', '30-45', '45-60', '60+'];
  const durCounts = Array(durLabels.length).fill(0);
  rows.forEach((t) => {
    const vSec = Number(t.trip_duration);
    const v = isFinite(vSec) ? vSec / 60 : NaN;
    if (!isFinite(v)) return;
    let idx = durBins.findIndex((b) => v < b);
    if (idx === -1) idx = durLabels.length - 1;
    durCounts[idx] += 1;
  });
  if (durationChart) durationChart.destroy();
  durationChart = new Chart(ctxDuration, {
    type: 'bar',
    data: { labels: durLabels, datasets: [{ label: 'Duration (min)', data: durCounts, backgroundColor: '#67b3ff' }] },
    options: { responsive: true, scales: { y: { beginAtZero: true } } }
  });

  // Distance histogram (km)
  const distBins = [0, 2, 5, 10, 20, 50];
  const distLabels = ['<2', '2-5', '5-10', '10-20', '20-50', '50+'];
  const distCounts = Array(distLabels.length).fill(0);
  rows.forEach((t) => {
    const v = Number(t.trip_distance);
    if (!isFinite(v)) return;
    let idx = distBins.findIndex((b) => v < b);
    if (idx === -1) idx = distLabels.length - 1;
    distCounts[idx] += 1;
  });
  if (distanceChart) distanceChart.destroy();
  distanceChart = new Chart(ctxDistance, {
    type: 'bar',
    data: { labels: distLabels, datasets: [{ label: 'Distance (km)', data: distCounts, backgroundColor: '#77d1b1' }] },
    options: { responsive: true, scales: { y: { beginAtZero: true } } }
  });

  // Speed histogram (km/h)
  const speedBins = [0, 10, 20, 30, 40, 60, 80];
  const speedLabels = ['<10', '10-20', '20-30', '30-40', '40-60', '60-80', '80+'];
  const speedCounts = Array(speedLabels.length).fill(0);
  rows.forEach((t) => {
    const v = Number(t.trip_speed_kmh);
    if (!isFinite(v)) return;
    let idx = speedBins.findIndex((b) => v < b);
    if (idx === -1) idx = speedLabels.length - 1;
    speedCounts[idx] += 1;
  });
  if (speedChart) speedChart.destroy();
  speedChart = new Chart(ctxSpeed, {
    type: 'bar',
    data: { labels: speedLabels, datasets: [{ label: 'Speed (km/h)', data: speedCounts, backgroundColor: '#f2a65a' }] },
    options: { responsive: true, scales: { y: { beginAtZero: true } } }
  });

  // Trips by hour
  const hourlyCounts = Array.from({ length: 24 }, () => 0);
  rows.forEach((t) => {
    const d = new Date(t.pickup_datetime);
    if (!isNaN(d)) {
      const h = d.getHours();
      if (h >= 0 && h < 24) hourlyCounts[h] += 1;
    }
  });
  if (hourChart) hourChart.destroy();
  hourChart = new Chart(ctxHour, {
    type: 'bar',
    data: { labels: Array.from({ length: 24 }, (_, i) => String(i)), datasets: [{ label: 'Trips by Hour', data: hourlyCounts, backgroundColor: '#4da2fb' }] },
    options: { responsive: true, scales: { y: { beginAtZero: true } } }
  });
}

function applyTheme(theme) {
  const t = theme || localStorage.getItem('theme') || 'light';
  if (t === 'dark') {
    document.body.setAttribute('data-theme', 'dark');
  } else {
    document.body.removeAttribute('data-theme');
  }
  localStorage.setItem('theme', t);
}

function toggleTheme() {
  const current = document.body.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
  const next = current === 'dark' ? 'light' : 'dark';
  applyTheme(next);
}

function updateStats(rows, totalFromServer) {
  const totalTrips = totalFromServer ?? rows.length;
  const avg = (arr) => arr.length ? arr.reduce((a,b)=>a+b,0)/arr.length : 0;
  const durs = rows.map(r => Number(r.trip_duration)).filter(Number.isFinite);
  const dists = rows.map(r => Number(r.trip_distance)).filter(Number.isFinite);
  const speeds = rows.map(r => Number(r.trip_speed_kmh)).filter(Number.isFinite);

  const elTotal = document.getElementById('total-trips');
  const elDur = document.getElementById('avg-duration');
  const elDist = document.getElementById('avg-distance');
  const elSpeed = document.getElementById('avg-speed');
  if (elTotal) elTotal.textContent = String(totalTrips);
  if (elDur) elDur.textContent = avg(durs).toFixed(1);
  if (elDist) elDist.textContent = avg(dists).toFixed(2);
  if (elSpeed) elSpeed.textContent = avg(speeds).toFixed(1);
}

async function init() {
  try {
    applyTheme();
    const toggleBtn = document.getElementById('theme-toggle');
    if (toggleBtn) toggleBtn.addEventListener('click', toggleTheme);

    // Hour slider display
    const hourSlider = document.getElementById('hour-slider');
    const hourValue = document.getElementById('hour-value');
    if (hourSlider && hourValue) {
      hourSlider.addEventListener('input', (e) => {
        hourValue.textContent = e.target.value;
      });
    }

    const result = await fetchTrips(200, 0);
    const rows = result?.data ?? [];
    lastRows = rows;
    updateTable(lastRows);
    updateStats(lastRows, result?.pagination?.total);
    renderCharts(lastRows);
    renderBusiestHoursTop5(lastRows);
    renderFareTrendDaily(lastRows);

    // Render manual algorithm charts
    const heatmap = await fetchHeatmapManual(3, 50000, 5000);
    renderHeatmap(heatmap.data || []);
    const routes = await fetchTopRoutesManual(3, 50000, 50);
    renderTopRoutes(routes.data || []);

    // Filters
    const btn = document.getElementById('apply-filters');
    if (btn) btn.addEventListener('click', () => {
      const df = document.getElementById('date-from')?.value || '';
      const dt = document.getElementById('date-to')?.value || '';
      const hour = Number(document.getElementById('hour-slider')?.value || '0');
      const dmin = parseFloat(document.getElementById('distance-min')?.value || '');
      const dmax = parseFloat(document.getElementById('distance-max')?.value || '');
      const fmin = parseFloat(document.getElementById('fare-min')?.value || '');
      const fmax = parseFloat(document.getElementById('fare-max')?.value || '');

      const filtered = lastRows.filter((t) => {
        // date range
        const d = new Date(t.pickup_datetime);
        if (!isNaN(d)) {
          if (df) {
            const start = new Date(df);
            if (d < start) return false;
          }
          if (dt) {
            const end = new Date(dt);
            end.setDate(end.getDate() + 1); // inclusive day end
            if (d >= end) return false;
          }
          if (Number.isFinite(hour)) {
            if (d.getHours() !== hour) return false;
          }
        }
        // distance
        const dist = Number(t.trip_distance);
        if (!isNaN(dmin) && isFinite(dmin) && isFinite(dist) && dist < dmin) return false;
        if (!isNaN(dmax) && isFinite(dmax) && isFinite(dist) && dist > dmax) return false;
        // fare (if present)
        const fare = t.fare_amount != null ? Number(t.fare_amount) : NaN;
        if (!isNaN(fmin) && isFinite(fmin) && isFinite(fare) && fare < fmin) return false;
        if (!isNaN(fmax) && isFinite(fmax) && isFinite(fare) && fare > fmax) return false;
        return true;
      });

      updateTable(filtered);
      updateStats(filtered, filtered.length);
      renderCharts(filtered);
      renderBusiestHoursTop5(filtered);
      renderFareTrendDaily(filtered);
    });
  } catch (err) {
    console.error(err);
    alert('Failed to load trips. Make sure the API is running on http://localhost:3000');
  }
}

document.addEventListener('DOMContentLoaded', init);
