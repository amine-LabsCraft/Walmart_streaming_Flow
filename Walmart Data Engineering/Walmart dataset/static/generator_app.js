const form = document.querySelector("#generator-form");
const customerInput = document.querySelector("#customer-id");
const intervalInput = document.querySelector("#interval");
const startButton = document.querySelector("#start-button");
const stopButton = document.querySelector("#stop-button");
const feedback = document.querySelector("#feedback");
const connectionState = document.querySelector("#connection-state");
const connectionLabel = document.querySelector("#connection-label");
const countdown = document.querySelector("#countdown");
const pulseOrbit = document.querySelector("#pulse-orbit");
const modeTitle = document.querySelector("#mode-title");
const ordersCount = document.querySelector("#orders-count");
const activeCustomer = document.querySelector("#active-customer");
const lastTotal = document.querySelector("#last-total");
const activityList = document.querySelector("#activity-list");
const modeInputs = [...document.querySelectorAll('input[name="customer_mode"]')];

const money = new Intl.NumberFormat("fr-FR", { style: "currency", currency: "CAD" });
let lastSignature = "";

function escapeHtml(value) {
  return String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#039;");
}

function selectedMode() {
  return modeInputs.find((input) => input.checked).value;
}

function updateCustomerInput() {
  customerInput.disabled = selectedMode() !== "fixed" || startButton.disabled;
  customerInput.required = selectedMode() === "fixed";
}
modeInputs.forEach((input) => input.addEventListener("change", updateCustomerInput));

function renderActivity(activity) {
  const signature = activity.map((item) => item.order_id).join(",");
  if (signature === lastSignature) return;
  lastSignature = signature;
  if (!activity.length) {
    activityList.innerHTML = '<div class="empty-state"><span>?</span><p>Démarrez le générateur pour voir les commandes arriver.</p></div>';
    return;
  }
  activityList.innerHTML = activity.map((item) =>
    '<article class="ticket">' +
      '<span class="ticket-number">#' + escapeHtml(item.order_id) + '</span>' +
      '<div class="ticket-route"><strong>' + escapeHtml(item.customer_name) + '  ' + escapeHtml(item.store_name) +
      '</strong><small>Employé choisi : ' + escapeHtml(item.employee_name) + ' · ID ' + escapeHtml(item.employee_id) + '</small></div>' +
      '<div class="ticket-meta"><strong>' + escapeHtml(item.order_item_count) + '</strong><span>articles</span></div>' +
      '<div class="ticket-meta"><strong>' + money.format(Number(item.total_amount)) + '</strong><span>total</span></div>' +
      '<div class="ticket-meta"><strong>' + new Date(item.generated_at).toLocaleTimeString("fr-FR") + '</strong><span>injectée</span></div>' +
    '</article>'
  ).join("");
}

function renderStatus(status) {
  const copy = {
    idle: ["Prêt", "En attente"],
    starting: ["Connexion.", "Démarrage"],
    running: ["Génération active", "Commande en approche"],
    stopping: ["Arrêt.", "Finalisation"],
    error: ["Erreur", "Action requise"]
  }[status.mode] || ["Prêt", "En attente"];
  const active = ["starting", "running", "stopping"].includes(status.mode);

  connectionLabel.textContent = copy[0];
  modeTitle.textContent = copy[1];
  connectionState.classList.toggle("running", status.mode === "running");
  connectionState.classList.toggle("error", status.mode === "error");
  pulseOrbit.classList.toggle("active", status.mode === "running");
  startButton.disabled = active;
  stopButton.disabled = !active;
  intervalInput.disabled = active;
  modeInputs.forEach((input) => { input.disabled = active; });
  updateCustomerInput();

  ordersCount.textContent = status.orders_generated;
  activeCustomer.textContent = status.active_customer_id
    ? "#" + status.active_customer_id + " · " + (status.active_customer_name || "")
    : "-";
  lastTotal.textContent = status.last_order ? money.format(Number(status.last_order.total_amount)) : "-";
  countdown.textContent = status.mode === "running"
    ? Math.max(0, Math.ceil(status.seconds_until_next ?? status.interval_seconds))
    : status.interval_seconds;

  if (status.last_error) feedback.textContent = status.last_error;
  else if (status.mode === "running") feedback.textContent = "Écriture directe dans Ghost. Les secrets restent côté serveur.";
  renderActivity(status.activity);
}

async function refresh() {
  try {
    const response = await fetch("/api/status", { cache: "no-store" });
    if (!response.ok) throw new Error("État indisponible.");
    renderStatus(await response.json());
  } catch (error) {
    connectionState.classList.add("error");
    connectionLabel.textContent = "Interface déconnectée";
    feedback.textContent = error.message;
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  feedback.textContent = "";
  const response = await fetch("/api/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      customer_mode: selectedMode(),
      customer_id: customerInput.value || null,
      interval_seconds: intervalInput.value
    })
  });
  const result = await response.json();
  feedback.textContent = result.message;
  await refresh();
});

stopButton.addEventListener("click", async () => {
  const response = await fetch("/api/stop", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: "{}"
  });
  const result = await response.json();
  feedback.textContent = result.message;
  await refresh();
});

updateCustomerInput();
refresh();
window.setInterval(refresh, 1000);
