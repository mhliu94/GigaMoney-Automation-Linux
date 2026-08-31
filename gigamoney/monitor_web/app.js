"use strict";

const state = { status: null, busy: null, csrfToken: null };
const ids = [
  "account-id", "server-id", "header-clock", "script-card", "script-status", "script-pid",
  "script-started", "script-duration", "script-message", "script-start", "script-stop",
  "emulator-card", "emulator-status", "emulator-avd", "emulator-device",
  "emulator-availability", "emulator-message", "emulator-start", "emulator-stop",
  "holdings-query-message", "holdings-query-count", "holdings-query-average",
  "holdings-query-fastest", "holdings-query-slowest", "health-overall", "health-summary",
  "health-machine-cpu-value", "health-machine-cpu-detail", "health-machine-cpu-level",
  "health-machine-memory-value", "health-machine-memory-detail", "health-machine-memory-level",
  "health-emulator-memory-value", "health-emulator-memory-detail", "health-emulator-memory-level",
  "health-adb-health-value", "health-adb-health-detail", "health-adb-health-level",
  "health-android-response-value", "health-android-response-detail", "health-android-response-level",
  "last-checked", "next-check", "refresh-status", "toast-region", "sign-out",
];
const el = Object.fromEntries(ids.map((id) => [id, document.getElementById(id)]));

function dateTime(value) {
  if (!value) return "—";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? "—" : new Intl.DateTimeFormat(undefined, {
    month: "short", day: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit",
  }).format(parsed);
}

function duration(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  let seconds = Math.max(0, Math.floor(Number(value)));
  const days = Math.floor(seconds / 86400); seconds %= 86400;
  const hours = Math.floor(seconds / 3600); seconds %= 3600;
  const minutes = Math.floor(seconds / 60); seconds %= 60;
  const clock = [hours, minutes, seconds].map((part) => String(part).padStart(2, "0")).join(":");
  return days ? `${days}d ${clock}` : clock;
}

function countdown(value) {
  if (!value) return "—";
  const seconds = Math.max(0, Math.ceil((new Date(value).getTime() - Date.now()) / 1000));
  return seconds ? `in ${String(Math.floor(seconds / 60)).padStart(2, "0")}:${String(seconds % 60).padStart(2, "0")}` : "due now";
}

function statusLabel(value) {
  return ({ running: "Running", stopped: "Stopped", starting: "Starting", booting: "Booting", attention: "Attention", unavailable: "Unavailable" })[value] || "Unknown";
}

function paintStatus(target, value) {
  const normalized = value || "unavailable";
  target.className = `status-pill is-${normalized}`;
  target.querySelector("strong").textContent = statusLabel(normalized);
}

function paintLevel(target, level, label) {
  const normalized = ["healthy", "under_load", "restart_recommended"].includes(level) ? level : "under_load";
  target.className = `health-level is-${normalized.replaceAll("_", "-")}`;
  const text = target.querySelector("strong, b");
  if (text) text.textContent = label || "Under load";
}

function seconds(value) {
  if (value === null || value === undefined || value === "") return "—";
  const number = Number(value);
  return Number.isFinite(number) ? `${number.toFixed(3)} s` : "—";
}

function renderQueries(data = {}) {
  const count = Number(data.sample_count) || 0;
  const total = Number(data.attempts_in_session) || count;
  el["holdings-query-count"].textContent = `${total} attempt${total === 1 ? "" : "s"}`;
  el["holdings-query-message"].textContent = data.message || "Query timing is unavailable.";
  el["holdings-query-average"].textContent = seconds(data.average_seconds);
  el["holdings-query-fastest"].textContent = seconds(data.fastest_seconds);
  el["holdings-query-slowest"].textContent = seconds(data.slowest_seconds);
}

function renderHealth(data = {}) {
  paintLevel(el["health-overall"], data.overall_level, data.overall_label);
  el["health-summary"].textContent = data.summary || "Health information is unavailable.";
  const names = {
    machine_cpu: "machine-cpu", machine_memory: "machine-memory", emulator_memory: "emulator-memory",
    adb_health: "adb-health", android_response: "android-response",
  };
  (data.metrics || []).forEach((metric) => {
    const name = names[metric.id];
    if (!name) return;
    el[`health-${name}-value`].textContent = metric.value || "Unavailable";
    el[`health-${name}-detail`].textContent = metric.detail || "No details available.";
    paintLevel(el[`health-${name}-level`], metric.level, metric.level_label);
  });
}

function buttons() {
  const runner = Boolean(state.status?.script?.running);
  const emulator = Boolean(state.status?.emulator?.running);
  const busy = Boolean(state.busy);
  el["script-start"].disabled = busy || runner;
  el["script-stop"].disabled = busy || !runner;
  el["emulator-start"].disabled = busy || emulator;
  el["emulator-stop"].disabled = busy || !emulator;
  el["refresh-status"].disabled = busy;
}

function render(data) {
  state.status = data;
  const script = data.script || {};
  const emulator = data.emulator || {};
  const account = data.account || {};
  el["account-id"].textContent = account.account_id || "Account not configured";
  el["server-id"].textContent = account.server_id || "GigaMoney server";
  paintStatus(el["script-status"], script.state);
  el["script-card"].dataset.state = script.state || "unavailable";
  el["script-pid"].textContent = script.running && script.pid ? String(script.pid) : "Not running";
  el["script-started"].textContent = script.running ? dateTime(script.started_at) : "—";
  el["script-duration"].textContent = script.running ? duration(script.duration_seconds) : "Not running";
  el["script-message"].textContent = script.message || "Runner status unavailable.";
  paintStatus(el["emulator-status"], emulator.state);
  el["emulator-card"].dataset.state = emulator.state || "unavailable";
  el["emulator-avd"].textContent = emulator.avd || "Not configured";
  el["emulator-device"].textContent = emulator.device || "Not configured";
  el["emulator-availability"].textContent = statusLabel(emulator.state);
  el["emulator-message"].textContent = emulator.message || "Emulator status unavailable.";
  renderQueries(data.holdings_queries);
  renderHealth(data.health);
  el["last-checked"].textContent = dateTime(data.checked_at);
  el["next-check"].textContent = countdown(data.next_check_at);
  buttons();
}

function toast(message, failed = false) {
  const node = document.createElement("div");
  node.className = `toast${failed ? " failed" : ""}`;
  node.textContent = message;
  el["toast-region"].appendChild(node);
  window.setTimeout(() => node.remove(), 6000);
}

async function fetchStatus(force = false) {
  el["refresh-status"].disabled = true;
  try {
    const response = await fetch(`/api/status${force ? "?refresh=1" : ""}`, { cache: "no-store", headers: { Accept: "application/json" } });
    if (response.status === 401) {
      window.location.replace("/login");
      return;
    }
    if (!response.ok) throw new Error(`Status request failed (${response.status}).`);
    render(await response.json());
  } catch (error) {
    toast(error.message || "Could not reach the local monitor.", true);
  } finally {
    buttons();
  }
}

async function action(button) {
  if (button.dataset.confirm && !window.confirm(button.dataset.confirm)) return;
  const name = button.dataset.action;
  const original = button.innerHTML;
  state.busy = name;
  button.textContent = "Working…";
  buttons();
  try {
    const response = await fetch(`/api/${name}`, {
      method: "POST", cache: "no-store",
      headers: {
        Accept: "application/json", "Content-Type": "application/json",
        "X-GigaMoney-Action": "1", "X-CSRF-Token": state.csrfToken,
      },
      body: "{}",
    });
    const payload = await response.json().catch(() => ({}));
    if (payload.status) render(payload.status);
    if (!response.ok || !payload.ok) throw new Error(payload.message || `Action failed (${response.status}).`);
    toast(payload.message || "Control action completed.");
  } catch (error) {
    toast(error.message || "Control action failed.", true);
  } finally {
    state.busy = null;
    button.innerHTML = original;
    buttons();
  }
}

document.querySelectorAll("[data-action]").forEach((button) => button.addEventListener("click", () => action(button)));
el["refresh-status"].addEventListener("click", () => fetchStatus(true));
el["sign-out"].addEventListener("click", async () => {
  el["sign-out"].disabled = true;
  try {
    await fetch("/api/logout", {
      method: "POST", cache: "no-store",
      headers: { "Content-Type": "application/json", "X-CSRF-Token": state.csrfToken },
      body: "{}",
    });
  } finally {
    window.location.replace("/login");
  }
});

window.setInterval(() => {
  const now = new Date();
  el["header-clock"].textContent = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }).format(now);
  if (state.status?.script?.running && state.status.script.started_at) {
    el["script-duration"].textContent = duration((now.getTime() - new Date(state.status.script.started_at).getTime()) / 1000);
  }
  if (state.status) el["next-check"].textContent = countdown(state.status.next_check_at);
}, 1000);
window.setInterval(() => fetchStatus(false), 60000);

async function initialize() {
  try {
    const response = await fetch("/api/session", { cache: "no-store", headers: { Accept: "application/json" } });
    if (!response.ok) {
      window.location.replace("/login");
      return;
    }
    const session = await response.json();
    state.csrfToken = session.csrf_token;
    await fetchStatus(false);
  } catch (_error) {
    toast("Could not establish a secure session.", true);
  }
}

initialize();
