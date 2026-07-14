const logEl = document.querySelector("#log");
const statusJsonEl = document.querySelector("#status-json");
const clientsEl = document.querySelector("#clients");
const modeEl = document.querySelector("#mode");
const serverModeEl = document.querySelector("#server-mode");

function log(message, data) {
  const now = new Date().toLocaleTimeString();
  const detail = data ? `\n${JSON.stringify(data, null, 2)}` : "";
  logEl.textContent = `[${now}] ${message}${detail}\n\n${logEl.textContent}`;
}

async function api(path, payload) {
  const response = await fetch(path, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(payload || {})
  });
  const data = await response.json();
  if (!response.ok || data.ok === false) {
    throw new Error(data.error || `HTTP ${response.status}`);
  }
  return data;
}

async function refreshStatus() {
  const response = await fetch("/api/status");
  const data = await response.json();
  if (!data.ok) return;
  renderStatus(data.status);
}

function renderStatus(status) {
  modeEl.value = status.mode;
  serverModeEl.textContent = `mode: ${status.mode}`;
  statusJsonEl.textContent = JSON.stringify(status, null, 2);
  clientsEl.innerHTML = "";

  if (!status.clients.length) {
    clientsEl.textContent = "No clients connected.";
    return;
  }

  for (const client of status.clients) {
    const node = document.createElement("div");
    node.className = "client";
    node.innerHTML = `
      <strong>${client.id}</strong>
      <div>control: ${client.control_connected ? "yes" : "no"}</div>
      <div>acquisition: ${client.acquisition_connected ? "yes" : "no"}</div>
      <div>multipmt: ${client.identity.multipmt_id || "-"}</div>
      <div>batch: ${client.identity.batch_id || "-"}</div>
    `;
    clientsEl.appendChild(node);
  }
}

document.body.addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;

  const action = button.dataset.action;
  button.disabled = true;

  try {
    let result;
    if (action === "connect") {
      result = await api("/api/connect", {
        num_clients: document.querySelector("#num-clients").value,
        control_port: document.querySelector("#control-port").value,
        acq_port: document.querySelector("#acq-port").value
      });
    } else if (action === "mode") {
      result = await api("/api/mode", {mode: modeEl.value});
    } else if (action === "hv") {
      result = await api("/api/hv", {
        command: document.querySelector("#hv-command").value,
        channels: document.querySelector("#hv-channels").value,
        value: document.querySelector("#hv-value").value
      });
    } else if (action === "rc") {
      result = await api("/api/rc", {
        command: document.querySelector("#rc-command").value,
        channels: document.querySelector("#rc-channels").value,
        address: document.querySelector("#rc-address").value,
        value: document.querySelector("#rc-value").value
      });
    } else if (action === "acq-start") {
      result = await api("/api/acquisition/start", {
        duration: document.querySelector("#acq-duration").value,
        type: document.querySelector("#acq-type").value,
        suffix: document.querySelector("#acq-suffix").value,
        run_id: document.querySelector("#acq-run-id").value,
        batch_id: document.querySelector("#acq-batch-id").value,
        force_compile: document.querySelector("#acq-force-compile").checked
      });
    } else if (action === "acq-stop") {
      result = await api("/api/acquisition/stop", {});
    }

    log(`${action} completed`, result);
    await refreshStatus();
  } catch (error) {
    log(`${action} failed: ${error.message}`);
  } finally {
    button.disabled = false;
  }
});

refreshStatus();
setInterval(refreshStatus, 3000);
