async function loadSummary() {
  const res = await fetch("http://localhost:5000/api/trips/summary");
  const data = await res.json();
  document.getElementById("summary").innerHTML = `
    <p>Average Distance: ${data.avg_distance.toFixed(2)} km</p>
    <p>Average Fare: $${data.avg_fare.toFixed(2)}</p>
    <p>Average Tip: $${data.avg_tip.toFixed(2)}</p>`;
}
loadSummary();

