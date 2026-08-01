const generatorForm = document.querySelector("#generator-form");
const intervalInput = document.querySelector("#interval");
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
const navItems = [...document.querySelectorAll("[data-panel-target]")];
const panels = [...document.querySelectorAll("[data-panel]")];
const eventFilters = [...document.querySelectorAll("[data-event-filter]")];

const money = new Intl.NumberFormat("fr-CA", {
  style: "currency",
  currency: "CAD"
});

let currentFilter = "all";
let latestActivity = [];
let lastSignature = "";

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, options);
  const result = await response.json().catch(() => ({
    ok: false,
    message: "Réponse serveur illisible."
  }));
  if (!response.ok) {
    throw new Error(result.message || "L’action n’a pas pu être terminée.");
  }
  return result;
}

function setPanel(panelName) {
  navItems.forEach((button) => {
    const selected = button.dataset.panelTarget === panelName;
    button.classList.toggle("active", selected);
    button.setAttribute("aria-selected", String(selected));
  });
  panels.forEach((panel) => {
    panel.hidden = panel.dataset.panel !== panelName;
  });
}

function emptyActivity(filter) {
  const copy = {
    all: "Démarrez les commandes ou générez un sign up pour alimenter le flux.",
    order: "Aucune commande dans cette session.",
    customer_signup: "Aucun sign up dans cette session."
  }[filter];
  return [
    '<div class="empty-state">',
    '<span class="empty-icon">⌁</span>',
    "<strong>Le flux est prêt</strong>",
    "<p>", escapeHtml(copy), "</p>",
    "</div>"
  ].join("");
}

function orderRow(item) {
  return [
    '<article class="event-row order-row">',
    '<div class="event-type"><span class="event-glyph">↗</span><div>',
    "<strong>Commande</strong><small>#", escapeHtml(item.order_id), "</small>",
    "</div></div>",
    '<div class="event-main"><strong>', escapeHtml(item.customer_name),
    '</strong><small>', escapeHtml(item.store_name), " · client #",
    escapeHtml(item.customer_id), "</small></div>",
    '<div class="event-context"><strong class="amount">',
    money.format(Number(item.total_amount)),
    '</strong><small>', escapeHtml(item.order_item_count), " articles · ",
    escapeHtml(item.employee_name), "</small></div>",
    '<div class="event-time"><strong>',
    new Date(item.generated_at).toLocaleTimeString("fr-FR", {
      hour: "2-digit",
      minute: "2-digit"
    }),
    '</strong><small>injectée</small></div>',
    "</article>"
  ].join("");
}

function signupRow(item) {
  const location = [item.city, item.province].filter(Boolean).join(", ");
  return [
    '<article class="event-row customer-row">',
    '<div class="event-type"><span class="event-glyph">＋</span><div>',
    "<strong>Sign up</strong><small>#", escapeHtml(item.customer_id), "</small>",
    "</div></div>",
    '<div class="event-main"><strong>', escapeHtml(item.customer_name),
    '</strong><small>', escapeHtml(item.email), "</small></div>",
    '<div class="event-context"><strong>', escapeHtml(location),
    '</strong><small>', escapeHtml(item.phone || "Client actif · Y"),
    "</small></div>",
    '<div class="event-time"><strong>',
    new Date(item.generated_at).toLocaleTimeString("fr-FR", {
      hour: "2-digit",
      minute: "2-digit"
    }),
    '</strong><small>inscrit</small></div>',
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
        item.event_type === "customer_signup"
          ? signupRow(item)
          : orderRow(item)
      ).join("")
    : emptyActivity(currentFilter);
}

function renderSignupPreview(customer) {
  if (!customer) {
    lastSignupPreview.hidden = true;
    return;
  }

  const nameParts = String(customer.customer_name || "")
    .split(" ")
    .filter(Boolean);
  signupPreviewInitials.textContent = nameParts
    .slice(0, 2)
    .map((part) => part[0])
    .join("")
    .toUpperCase() || "CL";
  signupPreviewName.textContent =
    "#" + customer.customer_id + " · " + customer.customer_name;
  signupPreviewEmail.textContent = customer.email;
  signupPreviewLocation.textContent =
    [customer.city, customer.province].filter(Boolean).join(", ");
  signupPreviewPhone.textContent = customer.phone || "Client actif · Y";
  lastSignupPreview.hidden = false;
}

function updateQuickTimes() {
  quickTimes.forEach((button) => {
    button.classList.toggle(
      "selected",
      Number(button.dataset.interval) === Number(intervalInput.value)
    );
  });
}

function renderStatus(status) {
  const copy = {
    idle: ["Prêt", "En attente"],
    starting: ["Connexion…", "Démarrage"],
    running: ["Générateur actif", "Prochaine commande"],
    stopping: ["Arrêt…", "Finalisation"],
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
  quickTimes.forEach((button) => {
    button.disabled = active;
  });

  ordersCount.textContent = status.orders_generated;
  signupsCount.textContent = status.customers_signed_up;
  activeCustomer.textContent = status.active_customer_id
    ? "#" + status.active_customer_id + " · " +
      (status.active_customer_name || "")
    : "—";
  lastTotal.textContent = status.last_order
    ? money.format(Number(status.last_order.total_amount))
    : "—";
  countdown.textContent = status.mode === "running"
    ? Math.max(
        0,
        Math.ceil(status.seconds_until_next ?? status.interval_seconds)
      )
    : status.interval_seconds;

  if (status.last_error) {
    generatorFeedback.textContent = status.last_error;
    generatorFeedback.dataset.tone = "error";
  } else if (status.mode === "running") {
    generatorFeedback.textContent =
      "Écriture continue dans PostgreSQL · sélection métier automatique.";
    generatorFeedback.dataset.tone = "success";
  }

  renderSignupPreview(status.last_signup);
  renderActivity(status.activity);
}

async function refresh() {
  try {
    const status = await requestJson("/api/status", { cache: "no-store" });
    renderStatus(status);
  } catch (error) {
    connectionState.classList.add("error");
    connectionLabel.textContent = "Interface déconnectée";
    generatorFeedback.textContent = error.message;
    generatorFeedback.dataset.tone = "error";
  }
}

navItems.forEach((button) => {
  button.addEventListener("click", () => {
    setPanel(button.dataset.panelTarget);
  });
});

eventFilters.forEach((button) => {
  button.addEventListener("click", () => {
    currentFilter = button.dataset.eventFilter;
    eventFilters.forEach((candidate) => {
      candidate.classList.toggle("active", candidate === button);
    });
    renderActivity(latestActivity, true);
  });
});

quickTimes.forEach((button) => {
  button.addEventListener("click", () => {
    intervalInput.value = button.dataset.interval;
    updateQuickTimes();
  });
});
intervalInput.addEventListener("input", updateQuickTimes);

generatorForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  generatorFeedback.textContent = "";
  try {
    const result = await requestJson("/api/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        interval_seconds: intervalInput.value
      })
    });
    generatorFeedback.textContent = result.message;
    generatorFeedback.dataset.tone = "success";
  } catch (error) {
    generatorFeedback.textContent = error.message;
    generatorFeedback.dataset.tone = "error";
  }
  await refresh();
});

stopButton.addEventListener("click", async () => {
  try {
    const result = await requestJson("/api/stop", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}"
    });
    generatorFeedback.textContent = result.message;
    generatorFeedback.dataset.tone = "success";
  } catch (error) {
    generatorFeedback.textContent = error.message;
    generatorFeedback.dataset.tone = "error";
  }
  await refresh();
});

signupButton.addEventListener("click", async () => {
  signupFeedback.textContent = "";
  signupButton.disabled = true;
  signupButton.innerHTML = '<span aria-hidden="true">◌</span> Génération…';

  try {
    const result = await requestJson("/api/customers/signup", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}"
    });
    signupFeedback.textContent = result.message;
    signupFeedback.dataset.tone = "success";
    renderSignupPreview(result.customer);
  } catch (error) {
    signupFeedback.textContent = error.message;
    signupFeedback.dataset.tone = "error";
  } finally {
    signupButton.disabled = false;
    signupButton.innerHTML =
      '<span aria-hidden="true">✦</span> Générer un sign up';
  }

  await refresh();
});

setPanel("orders");
updateQuickTimes();
refresh();
window.setInterval(refresh, 1000);