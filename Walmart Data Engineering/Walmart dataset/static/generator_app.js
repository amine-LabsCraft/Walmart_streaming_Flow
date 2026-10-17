const generatorForm = document.querySelector("#generator-form");
const intervalInput = document.querySelector("#interval");
const intervalError = document.querySelector("#interval-error");
const startButton = document.querySelector("#start-button");
const stopButton = document.querySelector("#stop-button");
const signupButton = document.querySelector("#signup-button");
const generatorFeedback = document.querySelector("#generator-feedback");
const signupFeedback = document.querySelector("#signup-feedback");
const connectionState = document.querySelector("#connection-state");
const connectionLabel = document.querySelector("#connection-label");
const countdown = document.querySelector("#countdown");
const pulseOrbit = document.querySelector("#pulse-orbit");
const modeTitle = document.querySelector("#mode-title");
const ordersCount = document.querySelector("#orders-count");
const signupsCount = document.querySelector("#signups-count");
const activeCustomer = document.querySelector("#active-customer");
const lastTotal = document.querySelector("#last-total");
const activityList = document.querySelector("#activity-list");
const lastSignupPreview = document.querySelector("#last-signup-preview");
const signupPreviewInitials = document.querySelector("#signup-preview-initials");
const signupPreviewName = document.querySelector("#signup-preview-name");
const signupPreviewEmail = document.querySelector("#signup-preview-email");
const signupPreviewLocation = document.querySelector("#signup-preview-location");
const signupPreviewPhone = document.querySelector("#signup-preview-phone");
const quickTimes = [...document.querySelectorAll("[data-interval]")];
const tabs = [...document.querySelectorAll("[role='tab']")];
const panels = [...document.querySelectorAll("[data-panel]")];
const eventFilters = [...document.querySelectorAll("[data-event-filter]")];

const MIN_INTERVAL = Number(intervalInput.min);
const MAX_INTERVAL = Number(intervalInput.max);
const BASE_TITLE = "Order Pulse · Pilotage source";

const money = new Intl.NumberFormat("fr-CA", {
  style: "currency",
  currency: "CAD"
});
// fr-FR renders a compact 24-hour clock (HH:MM:SS) for the activity list.
const clock = new Intl.DateTimeFormat("fr-FR", {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit"
});

const STATUS_COPY = {
  idle: { label: "Prêt", title: "En attente", tab: "" },
  starting: { label: "Connexion…", title: "Démarrage", tab: "Démarrage · " },
  running: { label: "Génération active", title: "Génération en cours", tab: "● Actif · " },
  stopping: { label: "Arrêt…", title: "Finalisation", tab: "Arrêt · " },
  error: { label: "Erreur", title: "Action requise", tab: "⚠ Erreur · " }
};

// Business vocabulary shown to users (database values stay in English).
const CHANNEL_LABELS = { "In-store": "En magasin", Online: "En ligne" };
const STATUS_LABELS = {
  Completed: "payée",
  Pending: "en préparation",
  Cancelled: "annulée",
  Returned: "retournée"
};

let currentFilter = "all";
let latestActivity = [];
let lastSignature = "";
let pendingAction = false;
let seenEvents = null;

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatTime(isoValue) {
  const date = new Date(isoValue);
  return Number.isNaN(date.getTime()) ? "—" : clock.format(date);
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);
  const result = await response.json().catch(() => ({
    ok: false,
    message: "Réponse du serveur illisible."
  }));
  if (!response.ok) {
    throw new Error(result.message || "L'action n'a pas pu être terminée.");
  }
  return result;
}

function setFeedback(element, message, tone) {
  element.textContent = message;
  if (tone) {
    element.dataset.tone = tone;
  } else {
    delete element.dataset.tone;
  }
}

function setBusy(button, busy) {
  button.disabled = busy;
  button.setAttribute("aria-busy", String(busy));
}

/* ---------------------------------------------------------------- */
/* Tabs (WAI-ARIA tabs pattern, automatic activation)                */
/* ---------------------------------------------------------------- */

function selectTab(tab, moveFocus = false) {
  tabs.forEach((candidate) => {
    const selected = candidate === tab;
    candidate.classList.toggle("active", selected);
    candidate.setAttribute("aria-selected", String(selected));
    candidate.tabIndex = selected ? 0 : -1;
  });
  panels.forEach((panel) => {
    panel.hidden = panel.dataset.panel !== tab.dataset.panelTarget;
  });
  if (moveFocus) tab.focus();
}

tabs.forEach((tab, index) => {
  tab.addEventListener("click", () => selectTab(tab));
  tab.addEventListener("keydown", (event) => {
    const keys = {
      ArrowDown: index + 1,
      ArrowRight: index + 1,
      ArrowUp: index - 1,
      ArrowLeft: index - 1,
      Home: 0,
      End: tabs.length - 1
    };
    if (!(event.key in keys)) return;
    event.preventDefault();
    const next = (keys[event.key] + tabs.length) % tabs.length;
    selectTab(tabs[next], true);
  });
});

/* ---------------------------------------------------------------- */
/* Activity stream                                                   */
/* ---------------------------------------------------------------- */

function emptyActivity(filter) {
  const copy = {
    all: "Démarrez les commandes ou inscrivez un client : les événements apparaîtront ici.",
    order: "Aucune commande pendant cette session.",
    customer_signup: "Aucune inscription pendant cette session."
  }[filter];
  return [
    '<div class="empty-state">',
    '<span class="empty-icon" aria-hidden="true">⌁</span>',
    "<strong>Aucune activité pour l'instant</strong>",
    "<p>", escapeHtml(copy), "</p>",
    "</div>"
  ].join("");
}

function eventKey(item) {
  return item.event_type + ":" + (item.order_id || item.customer_id) + ":" + item.generated_at;
}

function rowClass(item) {
  // Only events that were not on screen before get the entry animation.
  return seenEvents && !seenEvents.has(eventKey(item)) ? " is-new" : "";
}

function orderRow(item) {
  const items = Number(item.order_item_count);
  return [
    '<article class="event-row order-row', rowClass(item), '">',
    '<div class="event-type"><span class="event-glyph" aria-hidden="true">↗</span><div>',
    "<strong>Commande</strong><small>n° ", escapeHtml(item.order_id), "</small>",
    "</div></div>",
    '<div class="event-main"><strong>', escapeHtml(item.customer_name),
    "</strong><small>", escapeHtml(item.store_name), " · client n° ",
    escapeHtml(item.customer_id), "</small></div>",
    '<div class="event-context"><strong class="amount">',
    money.format(Number(item.total_amount)),
    ' <span class="pill pill-', item.channel === "Online" ? "online" : "store", '">',
    escapeHtml(CHANNEL_LABELS[item.channel] || item.channel), " · ",
    escapeHtml(STATUS_LABELS[item.status] || item.status), "</span>",
    "</strong><small>", escapeHtml(items), items > 1 ? " articles · " : " article · ",
    escapeHtml(item.employee_name), "</small></div>",
    '<div class="event-time"><strong>', formatTime(item.generated_at),
    "</strong><small>créée</small></div>",
    "</article>"
  ].join("");
}

function signupRow(item) {
  const location = [item.city, item.province].filter(Boolean).join(", ");
  return [
    '<article class="event-row customer-row', rowClass(item), '">',
    '<div class="event-type"><span class="event-glyph" aria-hidden="true">+</span><div>',
    "<strong>Inscription</strong><small>client n° ", escapeHtml(item.customer_id), "</small>",
    "</div></div>",
    '<div class="event-main"><strong>', escapeHtml(item.customer_name),
    "</strong><small>", escapeHtml(item.email), "</small></div>",
    '<div class="event-context"><strong>', escapeHtml(location),
    "</strong><small>", escapeHtml(item.phone || "Client actif"), "</small></div>",
    '<div class="event-time"><strong>', formatTime(item.generated_at),
    "</strong><small>inscrit</small></div>",
    "</article>"
  ].join("");
}

function renderActivity(activity, force = false) {
  latestActivity = activity;
  const filtered = currentFilter === "all"
    ? activity
    : activity.filter((item) => item.event_type === currentFilter);
  const signature = currentFilter + "|" + filtered
    .map((item) => [
      item.event_type,
      item.order_id || item.customer_id,
      item.generated_at
    ].join(":"))
    .join(",");

  if (!force && signature === lastSignature) return;
  lastSignature = signature;

  activityList.innerHTML = filtered.length
    ? filtered.map((item) =>
        item.event_type === "customer_signup" ? signupRow(item) : orderRow(item)
      ).join("")
    : emptyActivity(currentFilter);
  seenEvents = new Set(activity.map(eventKey));
}

eventFilters.forEach((button) => {
  button.addEventListener("click", () => {
    currentFilter = button.dataset.eventFilter;
    eventFilters.forEach((candidate) => {
      const selected = candidate === button;
      candidate.classList.toggle("active", selected);
      candidate.setAttribute("aria-pressed", String(selected));
    });
    renderActivity(latestActivity, true);
  });
});

/* ---------------------------------------------------------------- */
/* Status                                                            */
/* ---------------------------------------------------------------- */

function renderSignupPreview(customer) {
  if (!customer) {
    lastSignupPreview.hidden = true;
    return;
  }

  const nameParts = String(customer.customer_name || "").split(" ").filter(Boolean);
  signupPreviewInitials.textContent = nameParts
    .slice(0, 2)
    .map((part) => part[0])
    .join("")
    .toUpperCase() || "CL";
  signupPreviewName.textContent =
    "n° " + customer.customer_id + " · " + customer.customer_name;
  signupPreviewEmail.textContent = customer.email;
  signupPreviewLocation.textContent =
    [customer.city, customer.province].filter(Boolean).join(", ");
  signupPreviewPhone.textContent = customer.phone || "Client actif";
  lastSignupPreview.hidden = false;
}

function renderStatus(status) {
  const copy = STATUS_COPY[status.mode] || STATUS_COPY.idle;
  const active = ["starting", "running", "stopping"].includes(status.mode);

  connectionLabel.textContent = copy.label;
  modeTitle.textContent = copy.title;
  document.title = copy.tab + BASE_TITLE;
  connectionState.classList.toggle("running", status.mode === "running");
  connectionState.classList.toggle("error", status.mode === "error");
  pulseOrbit.classList.toggle("active", status.mode === "running");

  if (!pendingAction) {
    startButton.disabled = active;
    stopButton.disabled = !active;
  }
  intervalInput.disabled = active;
  quickTimes.forEach((button) => {
    button.disabled = active;
  });
  // While generating, show the interval actually in use (e.g. after a reload).
  if (active && Number(intervalInput.value) !== status.interval_seconds) {
    intervalInput.value = status.interval_seconds;
    updateQuickTimes();
  }

  ordersCount.textContent = status.orders_generated;
  signupsCount.textContent = status.customers_signed_up;
  activeCustomer.textContent = status.active_customer_id
    ? "n° " + status.active_customer_id + " · " + (status.active_customer_name || "")
    : "—";
  activeCustomer.title = activeCustomer.textContent;
  lastTotal.textContent = status.last_order
    ? money.format(Number(status.last_order.total_amount))
    : "—";
  countdown.textContent = status.mode === "running"
    ? Math.max(0, Math.ceil(status.seconds_until_next ?? status.interval_seconds))
    : status.interval_seconds;

  if (status.last_error) {
    setFeedback(generatorFeedback, "Erreur PostgreSQL : " + status.last_error, "error");
  } else if (status.mode === "running") {
    setFeedback(
      generatorFeedback,
      "Écriture en continu dans PostgreSQL, toutes les " + status.interval_seconds + " s.",
      "success"
    );
  } else if (status.mode === "idle" && generatorFeedback.dataset.tone === "success") {
    setFeedback(generatorFeedback, "Générateur arrêté.", null);
  }

  renderSignupPreview(status.last_signup);
  renderActivity(status.activity);
}

async function refresh() {
  try {
    const status = await requestJson("/api/status", { cache: "no-store" });
    renderStatus(status);
  } catch (error) {
    connectionState.classList.remove("running");
    connectionState.classList.add("error");
    connectionLabel.textContent = "Serveur injoignable";
    document.title = "⚠ Hors ligne · " + BASE_TITLE;
  }
}

/* ---------------------------------------------------------------- */
/* Interval                                                          */
/* ---------------------------------------------------------------- */

function updateQuickTimes() {
  quickTimes.forEach((button) => {
    const selected = Number(button.dataset.interval) === Number(intervalInput.value);
    button.classList.toggle("selected", selected);
    button.setAttribute("aria-pressed", String(selected));
  });
}

function validateInterval() {
  const raw = intervalInput.value.trim();
  const value = Number(raw);
  let message = "";

  if (raw === "" || !Number.isFinite(value)) {
    message = "Saisissez un nombre de secondes.";
  } else if (value < MIN_INTERVAL || value > MAX_INTERVAL) {
    message = "L'intervalle doit être compris entre " + MIN_INTERVAL
      + " et " + MAX_INTERVAL.toLocaleString("fr-CA") + " secondes.";
  }

  intervalError.textContent = message;
  intervalError.hidden = !message;
  intervalInput.setAttribute("aria-invalid", String(Boolean(message)));
  return message ? null : value;
}

quickTimes.forEach((button) => {
  button.addEventListener("click", () => {
    intervalInput.value = button.dataset.interval;
    validateInterval();
    updateQuickTimes();
  });
});

intervalInput.addEventListener("input", () => {
  updateQuickTimes();
  if (intervalInput.getAttribute("aria-invalid") === "true") validateInterval();
});
intervalInput.addEventListener("blur", validateInterval);

/* ---------------------------------------------------------------- */
/* Actions                                                           */
/* ---------------------------------------------------------------- */

generatorForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const interval = validateInterval();
  if (interval === null) {
    intervalInput.focus();
    return;
  }

  pendingAction = true;
  setBusy(startButton, true);
  setFeedback(generatorFeedback, "Connexion à PostgreSQL…", null);
  try {
    const result = await requestJson("/api/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ interval_seconds: interval })
    });
    setFeedback(generatorFeedback, result.message, "success");
  } catch (error) {
    setFeedback(generatorFeedback, error.message, "error");
  } finally {
    pendingAction = false;
    startButton.removeAttribute("aria-busy");
  }
  await refresh();
});

stopButton.addEventListener("click", async () => {
  pendingAction = true;
  setBusy(stopButton, true);
  try {
    const result = await requestJson("/api/stop", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}"
    });
    setFeedback(generatorFeedback, result.message, null);
  } catch (error) {
    setFeedback(generatorFeedback, error.message, "error");
  } finally {
    pendingAction = false;
    stopButton.removeAttribute("aria-busy");
  }
  await refresh();
  startButton.focus();
});

signupButton.addEventListener("click", async () => {
  setFeedback(signupFeedback, "", null);
  setBusy(signupButton, true);
  signupButton.innerHTML = '<span aria-hidden="true">◌</span> Inscription en cours…';

  try {
    const result = await requestJson("/api/customers/signup", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}"
    });
    setFeedback(signupFeedback, result.message, "success");
    renderSignupPreview(result.customer);
  } catch (error) {
    setFeedback(signupFeedback, error.message, "error");
  } finally {
    setBusy(signupButton, false);
    signupButton.innerHTML = '<span aria-hidden="true">+</span> Inscrire un client';
  }

  await refresh();
});

selectTab(tabs[0]);
updateQuickTimes();
refresh();
window.setInterval(refresh, 1000);
