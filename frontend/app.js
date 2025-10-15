// Frontend Dashboard logic wired to local API
const API_BASE_URL = 'http://localhost:5050/api';

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Request failed: ${res.status}`);
  return res.json();
}

function buildQuery(params) {
  const q = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') q.set(k, v);
  });
  return q.toString();
}

async function fetchTrips(filters = {}, limit = 50, offset = 0) {
  const qs = buildQuery({
    from: filters.date_from,
    to: filters.date_to,
    hour: filters.hour,
    min_distance: filters.distance_min,
    max_distance: filters.distance_max,
    min_fare: filters.fare_min,
    max_fare: filters.fare_max,
    limit,
    offset
  });
  const url = `${API_BASE_URL}/trips?${qs}`;
  const data = await fetchJSON(url);
  return data;
}

async function fetchSummary(filters = {}) {
  const qs = buildQuery({
    from: filters.date_from,
    to: filters.date_to,
    hour: filters.hour,
  });
  const url = `${API_BASE_URL}/stats/summary?${qs}`;
  return fetchJSON(url);
}

function updateTable(trips) {
  const tbody = document.querySelector('#data-table tbody');
  tbody.innerHTML = '';
  trips.forEach((trip) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${trip.id ?? ''}</td>
      <td>${trip.pickup_datetime ?? ''}</td>
      <td>${trip.dropoff_datetime ?? ''}</td>
      <td>$${(trip.fare_amount ?? 0).toFixed ? (trip.fare_amount).toFixed(2) : trip.fare_amount}</td>
      <td>${trip.trip_distance_km ?? ''}</td>
      <td>${trip.speed_kmh ?? ''}</td>
      <td><button data-id="${trip.id}">View</button></td>
    `;
    tr.querySelector('button').addEventListener('click', () => viewDetails(trip.id));
    tbody.appendChild(tr);
  });
}

function updateSummaryCards(stats) {
  document.querySelector('#total-trips span').textContent = stats?.totals?.trips ?? 0;
  document.querySelector('#avg-fare span').textContent = `$${(stats?.averages?.fare_amount ?? 0).toFixed(2)}`;
  document.querySelector('#avg-speed span').textContent = `${(stats?.averages?.speed_kmh ?? 0).toFixed(1)} km/h`;
  document.querySelector('#total-revenue span').textContent = `$${(stats?.totals?.revenue ?? 0).toFixed(2)}`;
}

let hourChart, fareChart;
function updateCharts(stats) {
  const ctxHour = document.getElementById('trips-per-hour-chart').getContext('2d');
  const ctxFare = document.getElementById('fare-distribution-chart').getContext('2d');

  const hourly = stats?.distributions?.hourly_trips || {};
  const labelsHour = Array.from({length: 24}, (_, i) => String(i));
  const dataHour = labelsHour.map(h => hourly[h] ?? 0);

  if (hourChart) hourChart.destroy();
  hourChart = new Chart(ctxHour, {
    type: 'bar',
    data: {
      labels: labelsHour,
      datasets: [{ label: 'Trips per Hour', data: dataHour, backgroundColor: '#4e79a7' }]
    },
    options: { responsive: true, scales: { y: { beginAtZero: true } } }
  });

  const fareBins = stats?.distributions?.fare_histogram || {"0-10":0, "10-20":0, "20-30":0, "30+":0};
  const labelsFare = Object.keys(fareBins);
  const dataFare = labelsFare.map(k => fareBins[k] ?? 0);
  if (fareChart) fareChart.destroy();
  fareChart = new Chart(ctxFare, {
    type: 'bar',
    data: {
      labels: labelsFare,
      datasets: [{ label: 'Fare Distribution', data: dataFare, backgroundColor: '#f28e2c' }]
    },
    options: { responsive: true, scales: { y: { beginAtZero: true } } }
  });
}

function getFilters() {
  return {
    date_from: document.getElementById('date-from').value,
    date_to: document.getElementById('date-to').value,
    hour: document.getElementById('hour-slider').value,
    distance_min: document.getElementById('distance-min').value,
    distance_max: document.getElementById('distance-max').value,
    fare_min: document.getElementById('fare-min').value,
    fare_max: document.getElementById('fare-max').value,
  };
}

async function loadData(page = 1, perPage = 50) {
  const filters = getFilters();
  const offset = (page - 1) * perPage;
  const tripResp = await fetchTrips(filters, perPage, offset);
  updateTable(tripResp.trips || []);

  const stats = await fetchSummary(filters);
  updateSummaryCards(stats);
  updateCharts(stats);

  renderPagination(page, perPage, tripResp.total || 0);
}

function renderPagination(page, perPage, total) {
  const container = document.getElementById('pagination');
  const totalPages = Math.max(1, Math.ceil(total / perPage));
  container.innerHTML = `
    <button ${page<=1?'disabled':''} id="prev">Prev</button>
    <span>Page ${page} / ${totalPages}</span>
    <button ${page>=totalPages?'disabled':''} id="next">Next</button>
  `;
  document.getElementById('prev').onclick = () => loadData(page-1, perPage);
  document.getElementById('next').onclick = () => loadData(page+1, perPage);
}

function viewDetails(tripId) {
  if (!tripId) return;
  fetchJSON(`${API_BASE_URL}/trips/${tripId}`).then((trip) => {
    alert(`Trip ${trip.id}\nFare: $${trip.fare_amount}\nDistance: ${trip.trip_distance_km} km\nSpeed: ${trip.speed_kmh} km/h`);
  });
}

document.getElementById('apply-filters').addEventListener('click', () => loadData(1));

// Initial load
loadData(1);
