const API_BASE = 'http://127.0.0.1:5001';
let lastRows = [];
let isLoading = false;
let retryCount = 0;
const MAX_RETRIES = 3;
let useMockData = false;

// Test API connectivity
async function testApiConnectivity() {
  try {
    console.log('Testing API connectivity...');
    const response = await fetch(`${API_BASE}/health`, {
      method: 'GET',
      headers: { 'Accept': 'application/json' },
      mode: 'cors',
      cache: 'no-cache'
    });
    
    if (!response.ok) {
      throw new Error(`Health check failed: ${response.status}`);
    }
    
    const health = await response.json();
    console.log('API Health Check:', health);
    return health;
  } catch (error) {
    console.error('API connectivity test failed:', error);
    throw error;
  }
}

// Mock data for testing when API is not available
function getMockData() {
  const mockTrips = [];
  const now = new Date();
  
  for (let i = 0; i < 50; i++) {
    const pickupTime = new Date(now.getTime() - Math.random() * 7 * 24 * 60 * 60 * 1000);
    const duration = 300 + Math.random() * 1800; // 5-35 minutes
    const dropoffTime = new Date(pickupTime.getTime() + duration * 1000);
    const distance = 1 + Math.random() * 15; // 1-16 km
    const speed = distance / (duration / 3600); // km/h
    
    mockTrips.push({
      id: i + 1,
      pickup_datetime: pickupTime.toISOString(),
      dropoff_datetime: dropoffTime.toISOString(),
      trip_duration_seconds: duration,
      trip_distance_km: distance,
      trip_speed_kmh: speed,
      passenger_count: Math.floor(Math.random() * 4) + 1,
      fare_amount: 5 + Math.random() * 25,
      fare_per_km: (5 + Math.random() * 25) / distance
    });
  }
  
  return {
    data: mockTrips,
    pagination: { total: mockTrips.length, limit: 200, offset: 0 }
  };
}

async function fetchTrips(limit = 200, offset = 0, start = null, end = null) {
  const params = new URLSearchParams({ limit: String(limit), offset: String(offset) });
  if (start) params.set('start', start);
  if (end) params.set('end', end);
  
  const url = `${API_BASE}/api/trips?${params.toString()}`;
  console.log('Fetching trips from:', url);
  
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        'Accept': 'application/json'
      },
      mode: 'cors',
      cache: 'no-cache'
    });
    
    console.log('Response status:', res.status, res.statusText);
    
    if (!res.ok) {
      const errorText = await res.text();
      console.error('Response error text:', errorText);
      throw new Error(`HTTP ${res.status}: ${res.statusText} - ${errorText}`);
    }
    
    const data = await res.json();
    console.log('Received data:', { 
      totalTrips: data.pagination?.total, 
      dataLength: data.data?.length,
      firstTrip: data.data?.[0] ? Object.keys(data.data[0]) : 'No trips'
    });
    
    retryCount = 0; // Reset retry count on success
    return data;
  } catch (error) {
    console.error('Fetch trips error:', error);
    console.error('Error details:', {
      name: error.name,
      message: error.message,
      stack: error.stack
    });
    throw new Error(`Failed to fetch trips: ${error.message}`);
  }
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
  const url = `${API_BASE}/api/heatmap-manual?${params.toString()}`;
  console.log('Fetching heatmap from:', url);
  
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        'Accept': 'application/json'
      },
      mode: 'cors',
      cache: 'no-cache'
    });
    
    if (!res.ok) {
      const errorText = await res.text();
      throw new Error(`HTTP ${res.status}: ${res.statusText} - ${errorText}`);
    }
    
    const data = await res.json();
    console.log('Received heatmap data:', { dataLength: data.data?.length });
    return data;
  } catch (error) {
    console.error('Fetch heatmap error:', error);
    throw new Error(`Failed to fetch heatmap: ${error.message}`);
  }
}

async function fetchTopRoutesManual(precision = 3, limitRows = 50000, k = 10) {
  const params = new URLSearchParams({ precision: String(precision), limitRows: String(limitRows), k: String(k) });
  const url = `${API_BASE}/api/top-routes-manual?${params.toString()}`;
  console.log('Fetching top routes from:', url);
  
  try {
    const res = await fetch(url, {
      method: 'GET',
      headers: {
        'Accept': 'application/json'
      },
      mode: 'cors',
      cache: 'no-cache'
    });
    
    if (!res.ok) {
      const errorText = await res.text();
      throw new Error(`HTTP ${res.status}: ${res.statusText} - ${errorText}`);
    }
    
    const data = await res.json();
    console.log('Received routes data:', { dataLength: data.data?.length });
    return data;
  } catch (error) {
    console.error('Fetch top routes error:', error);
    throw new Error(`Failed to fetch top routes: ${error.message}`);
  }
}

function updateTable(rows) {
  const tbody = document.getElementById('trips-tbody');
  if (!tbody) return;
  tbody.innerHTML = '';
  rows.forEach((t) => {
    const dSec = t.trip_duration_seconds != null ? Number(t.trip_duration_seconds) : null;
    const durationMin = dSec != null && isFinite(dSec) ? dSec / 60 : null;
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${t.id ?? ''}</td>
      <td>${t.pickup_datetime ?? ''}</td>
      <td>${t.dropoff_datetime ?? ''}</td>
      <td>${durationMin != null ? Number(durationMin).toFixed(1) : '-'}</td>
      <td>${t.trip_distance_km != null ? Number(t.trip_distance_km).toFixed(2) : '-'}</td>
      <td>${t.trip_speed_kmh != null ? Number(t.trip_speed_kmh).toFixed(1) : '-'}</td>
      <td>${t.passenger_count ?? '-'}</td>
    `;
    tbody.appendChild(tr);
  });
}

let durationChart, distanceChart, speedChart, hourChart, heatmapChart, topRoutesChart, busiestHoursChart, fareTrendChart;
function renderHeatmap(data) {
  const ctx = document.getElementById('heatmapChart')?.getContext('2d');
  if (!ctx || !data || data.length === 0) return;
  
  // Create a simple scatter plot to represent the heatmap
  const points = data.slice(0, 100).map(d => ({
    x: d.lon,
    y: d.lat,
    v: d.count
  }));
  
  if (heatmapChart) heatmapChart.destroy();
  heatmapChart = new Chart(ctx, {
    type: 'scatter',
    data: {
      datasets: [{
        label: 'Pickup Locations',
        data: points,
        backgroundColor: points.map(p => `rgba(255, ${Math.max(0, 255 - p.v * 5)}, 0, 0.6)`),
        pointRadius: points.map(p => Math.min(10, Math.max(2, p.v / 100)))
      }]
    },
    options: {
      responsive: true,
      scales: {
        x: { title: { display: true, text: 'Longitude' } },
        y: { title: { display: true, text: 'Latitude' } }
      },
      plugins: {
        tooltip: {
          callbacks: {
            label: function(context) {
              const point = context.raw;
              return `Trips: ${point.v}, Lat: ${point.y}, Lon: ${point.x}`;
            }
          }
        }
      }
    }
  });
}

function renderTopRoutes(data) {
  const ctx = document.getElementById('topRoutesChart')?.getContext('2d');
  if (!ctx || !data || data.length === 0) return;
  
  // Create a bar chart showing top routes by count
  const labels = data.slice(0, 10).map((d, i) => `Route ${i + 1}`);
  const counts = data.slice(0, 10).map(d => d.count);
  
  if (topRoutesChart) topRoutesChart.destroy();
  topRoutesChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: 'Trip Count',
        data: counts,
        backgroundColor: '#8b5cf6',
        borderColor: '#7c3aed',
        borderWidth: 1
      }]
    },
    options: {
      responsive: true,
      scales: {
        y: { beginAtZero: true }
      },
      plugins: {
        tooltip: {
          callbacks: {
            afterLabel: function(context) {
              const route = data[context.dataIndex];
              return [
                `From: ${route.pickup_lat}, ${route.pickup_lon}`,
                `To: ${route.dropoff_lat}, ${route.dropoff_lon}`,
                `Avg Distance: ${route.avg_distance}km`,
                `Avg Fare: $${route.avg_fare}`
              ];
            }
          }
        }
      }
    }
  });
}

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
    const vSec = Number(t.trip_duration_seconds);
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
    const v = Number(t.trip_distance_km);
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
  const durs = rows.map(r => Number(r.trip_duration_seconds)).filter(Number.isFinite);
  const dists = rows.map(r => Number(r.trip_distance_km)).filter(Number.isFinite);
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

function showLoading(show = true) {
  const loadingElements = document.querySelectorAll('.loading-indicator');
  loadingElements.forEach(el => {
    el.style.display = show ? 'block' : 'none';
  });
  
  // Show/hide main content
  const mainContent = document.querySelectorAll('.stats-grid, .charts-grid, .table-container');
  mainContent.forEach(el => {
    el.style.opacity = show ? '0.5' : '1';
    el.style.pointerEvents = show ? 'none' : 'auto';
  });
}

function showError(message, isRetryable = false) {
  const errorContainer = document.getElementById('error-container');
  if (errorContainer) {
    errorContainer.innerHTML = `
      <div class="error-message">
        <h3>⚠️ Connection Error</h3>
        <p>${message}</p>
        ${isRetryable ? '<button id="retry-btn" class="btn">Retry</button>' : ''}
        <details style="margin-top: 10px;">
          <summary>Troubleshooting</summary>
          <ul style="margin: 10px 0; padding-left: 20px;">
            <li>Make sure the API server is running on http://localhost:5001</li>
            <li>Check that the database is connected</li>
            <li>Try refreshing the page</li>
            <li>Check the browser console for more details</li>
          </ul>
        </details>
      </div>
    `;
    errorContainer.style.display = 'block';
    
    if (isRetryable) {
      const retryBtn = document.getElementById('retry-btn');
      if (retryBtn) {
        retryBtn.addEventListener('click', () => {
          errorContainer.style.display = 'none';
          init();
        });
      }
    }
  }
}

function hideError() {
  const errorContainer = document.getElementById('error-container');
  if (errorContainer) {
    errorContainer.style.display = 'none';
  }
}

async function retryWithBackoff(fn, maxRetries = MAX_RETRIES) {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (error) {
      if (i === maxRetries - 1) throw error;
      
      const delay = Math.min(1000 * Math.pow(2, i), 5000); // Exponential backoff, max 5s
      console.log(`Retry ${i + 1}/${maxRetries} in ${delay}ms...`);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
}

async function init() {
  try {
    hideError();
    showLoading(true);
    isLoading = true;
    
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

    // Test API connectivity first
    let apiHealth;
    try {
      apiHealth = await testApiConnectivity();
      console.log('API is healthy:', apiHealth);
    } catch (connectivityError) {
      console.warn('API connectivity failed, will use mock data:', connectivityError.message);
      useMockData = true;
    }

    // Fetch main data with retry
    let result;
    try {
      if (useMockData) {
        throw new Error('Using mock data due to API connectivity issues');
      }
      result = await retryWithBackoff(() => fetchTrips(200, 0));
    } catch (apiError) {
      console.warn('API not available, using mock data:', apiError.message);
      useMockData = true;
      result = getMockData();
      
      // Show a warning that we're using mock data
      const warningContainer = document.getElementById('error-container');
      if (warningContainer) {
        warningContainer.innerHTML = `
          <div class="warning-message">
            <h3>⚠️ Using Demo Data</h3>
            <p>Unable to connect to the API server. Displaying sample data for demonstration.</p>
            <p><small>API Status: ${apiError.message}</small></p>
            <p><small>To see real data, make sure the API server is running on http://localhost:5001</small></p>
          </div>
        `;
        warningContainer.style.display = 'block';
        warningContainer.style.background = 'linear-gradient(135deg, #fef3c7 0%, #fde68a 100%)';
        warningContainer.style.borderColor = '#f59e0b';
      }
    }
    
    const rows = result?.data ?? [];
    lastRows = rows;
    
    console.log('Dashboard initialized with', rows.length, 'trips');
    
    updateTable(lastRows);
    updateStats(lastRows, result?.pagination?.total);
    renderCharts(lastRows);
    renderBusiestHoursTop5(lastRows);
    renderFareTrendDaily(lastRows);

    // Render manual algorithm charts with retry
    if (useMockData) {
      // Generate mock heatmap data
      const mockHeatmapData = [];
      for (let i = 0; i < 20; i++) {
        mockHeatmapData.push({
          lat: 40.7 + (Math.random() - 0.5) * 0.2,
          lon: -74.0 + (Math.random() - 0.5) * 0.2,
          count: Math.floor(Math.random() * 1000) + 100
        });
      }
      renderHeatmap(mockHeatmapData);
      
      // Generate mock routes data
      const mockRoutesData = [];
      for (let i = 0; i < 10; i++) {
        mockRoutesData.push({
          pickup_lat: 40.7 + (Math.random() - 0.5) * 0.2,
          pickup_lon: -74.0 + (Math.random() - 0.5) * 0.2,
          dropoff_lat: 40.7 + (Math.random() - 0.5) * 0.2,
          dropoff_lon: -74.0 + (Math.random() - 0.5) * 0.2,
          count: Math.floor(Math.random() * 500) + 50,
          avg_distance: 2 + Math.random() * 10,
          avg_fare: 8 + Math.random() * 15
        });
      }
      renderTopRoutes(mockRoutesData);
    } else {
      try {
        const heatmap = await retryWithBackoff(() => fetchHeatmapManual(3, 50000, 5000));
        renderHeatmap(heatmap.data || []);
      } catch (heatmapError) {
        console.warn('Failed to load heatmap data:', heatmapError.message);
      }
      
      try {
        const routes = await retryWithBackoff(() => fetchTopRoutesManual(3, 50000, 50));
        renderTopRoutes(routes.data || []);
      } catch (routesError) {
        console.warn('Failed to load routes data:', routesError.message);
      }
    }

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
        const dist = Number(t.trip_distance_km);
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
    
    showLoading(false);
    isLoading = false;
    
  } catch (err) {
    console.error('Init error:', err);
    console.error('Full error details:', {
      name: err.name,
      message: err.message,
      stack: err.stack
    });
    showLoading(false);
    isLoading = false;
    
    const isNetworkError = err.message.includes('fetch') || err.message.includes('Failed to fetch') || err.message.includes('NetworkError') || err.message.includes('CORS');
    const errorMessage = isNetworkError 
      ? 'Unable to connect to the API server. Please make sure the server is running on http://localhost:5001 and CORS is properly configured.'
      : `Failed to load data: ${err.message}`;
    
    showError(errorMessage, true);
  }
}

document.addEventListener('DOMContentLoaded', init);
