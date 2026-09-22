/**
 * FlashBite Live Order Tracker - Frontend Application
 * Polls Consumer A's /state endpoint every 2 seconds and dynamically renders active orders.
 */

const API_BASE_URL = window.location.origin.startsWith("http")
  ? window.location.origin
  : "http://localhost:5000";

const STATE_ENDPOINT = `${API_BASE_URL}/state`;
const POLL_INTERVAL_MS = 2000;

const STEPS = ["PLACED", "CONFIRMED", "PREPARING", "OUT_FOR_DELIVERY", "DELIVERED"];

// DOM Elements
const connectionPill = document.getElementById("connection-pill");
const connectionText = document.getElementById("connection-text");
const lastUpdatedTime = document.getElementById("last-updated-time");
const errorBanner = document.getElementById("error-banner");
const loadingState = document.getElementById("loading-state");
const emptyState = document.getElementById("empty-state");
const ordersGrid = document.getElementById("orders-grid");
const ordersBadge = document.getElementById("orders-badge");

const totalActiveCount = document.getElementById("total-active-count");
const placedCount = document.getElementById("placed-count");
const kitchenCount = document.getElementById("kitchen-count");
const deliveryCount = document.getElementById("delivery-count");

let isInitialLoad = true;
let previousOrderIds = new Set();

/**
 * Format status for human display.
 */
function formatStatusLabel(status) {
  switch (status) {
    case "OUT_FOR_DELIVERY":
      return "Out for Delivery";
    case "PREPARING":
      return "Preparing in Kitchen";
    case "CONFIRMED":
      return "Order Confirmed";
    case "PLACED":
      return "Order Placed";
    case "DELIVERED":
      return "Delivered";
    default:
      return status;
  }
}

/**
 * Calculate progress percentage for stepper track.
 */
function getProgressPercentage(status) {
  const index = STEPS.indexOf(status);
  if (index <= 0) return 0;
  return (index / (STEPS.length - 1)) * 100;
}

/**
 * Update UI connection indicators.
 */
function setConnectionState(status) {
  connectionPill.className = `status-pill ${status}`;
  if (status === "online") {
    connectionText.textContent = "Live Stream";
    errorBanner.classList.add("hidden");
  } else if (status === "offline") {
    connectionText.textContent = "Offline";
    errorBanner.classList.remove("hidden");
  } else {
    connectionText.textContent = "Connecting...";
  }
}

/**
 * Render an individual order card HTML.
 */
function createOrderCardHtml(order) {
  const currentStepIdx = STEPS.indexOf(order.status);
  const progressPercent = getProgressPercentage(order.status);
  const statusClass = (order.status || "").toLowerCase();

  const stepNodes = STEPS.map((step, idx) => {
    let nodeClass = "step-node";
    let icon = idx + 1;
    if (idx < currentStepIdx) {
      nodeClass += " completed";
      icon = "✓";
    } else if (idx === currentStepIdx) {
      nodeClass += " active";
    }
    return `<div class="${nodeClass}">${icon}</div>`;
  }).join("");

  const itemsHtml = (order.items || [])
    .map(item => `<span class="item-pill">${escapeHtml(item)}</span>`)
    .join("");

  return `
    <article class="order-card" id="card-${order.order_id}">
      <div class="card-top">
        <div class="order-id-group">
          <span class="order-id">${escapeHtml(order.order_id)}</span>
          <span class="customer-name">👤 ${escapeHtml(order.customer_name || "Guest")}</span>
        </div>
        <span class="status-badge ${statusClass}">
          ${escapeHtml(formatStatusLabel(order.status))}
        </span>
      </div>

      <div class="restaurant-info">
        <span class="restaurant-icon">🏪</span>
        <span>${escapeHtml(order.restaurant || "Restaurant")}</span>
      </div>

      <div class="stepper-container">
        <div class="stepper-track">
          <div class="stepper-line"></div>
          <div class="stepper-line-fill" style="width: calc(${progressPercent}% - 8px);"></div>
          ${stepNodes}
        </div>
        <div class="step-labels">
          <span class="${currentStepIdx === 0 ? 'active-label' : ''}">Placed</span>
          <span class="${currentStepIdx === 1 ? 'active-label' : ''}">Confirmed</span>
          <span class="${currentStepIdx === 2 ? 'active-label' : ''}">Kitchen</span>
          <span class="${currentStepIdx === 3 ? 'active-label' : ''}">Delivery</span>
          <span class="${currentStepIdx === 4 ? 'active-label' : ''}">Delivered</span>
        </div>
      </div>

      <div class="items-list">
        ${itemsHtml}
      </div>

      <div class="card-bottom">
        <span class="eta-pill">⏱️ Est. ${order.estimated_delivery_minutes || 30} mins</span>
        <span class="mono">${order.timestamp ? new Date(order.timestamp).toLocaleTimeString() : ""}</span>
      </div>
    </article>
  `;
}

/**
 * Basic XSS sanitizer.
 */
function escapeHtml(str) {
  if (typeof str !== "string") return "";
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

/**
 * Updates summary metrics counters.
 */
function updateMetrics(orders) {
  const orderList = Object.values(orders);
  const total = orderList.length;

  let placed = 0;
  let kitchen = 0;
  let delivery = 0;

  orderList.forEach(order => {
    if (order.status === "PLACED") placed++;
    else if (order.status === "CONFIRMED" || order.status === "PREPARING") kitchen++;
    else if (order.status === "OUT_FOR_DELIVERY") delivery++;
  });

  totalActiveCount.textContent = total;
  ordersBadge.textContent = total;
  placedCount.textContent = placed;
  kitchenCount.textContent = kitchen;
  deliveryCount.textContent = delivery;
}

/**
 * Main polling fetch function.
 */
async function fetchState() {
  try {
    const response = await fetch(STATE_ENDPOINT, { cache: "no-cache" });
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const state = await response.json();
    setConnectionState("online");

    const now = new Date();
    lastUpdatedTime.textContent = now.toLocaleTimeString();

    updateMetrics(state);

    const activeOrderIds = Object.keys(state);

    if (isInitialLoad) {
      loadingState.classList.add("hidden");
      isInitialLoad = false;
    }

    if (activeOrderIds.length === 0) {
      ordersGrid.classList.add("hidden");
      emptyState.classList.remove("hidden");
      ordersGrid.innerHTML = "";
    } else {
      emptyState.classList.add("hidden");
      ordersGrid.classList.remove("hidden");

      // Smoothly update or append cards
      activeOrderIds.forEach(orderId => {
        const orderData = state[orderId];
        const existingCard = document.getElementById(`card-${orderId}`);
        const newCardHtml = createOrderCardHtml(orderData);

        if (existingCard) {
          // If content changed, replace card innerHTML
          const tempDiv = document.createElement("div");
          tempDiv.innerHTML = newCardHtml;
          const freshCard = tempDiv.firstElementChild;
          if (existingCard.innerHTML !== freshCard.innerHTML) {
            existingCard.innerHTML = freshCard.innerHTML;
          }
        } else {
          // Append new card
          const tempDiv = document.createElement("div");
          tempDiv.innerHTML = newCardHtml;
          ordersGrid.appendChild(tempDiv.firstElementChild);
        }
      });

      // Remove cards for orders that completed (DELIVERED) and left state
      previousOrderIds.forEach(prevId => {
        if (!state[prevId]) {
          const finishedCard = document.getElementById(`card-${prevId}`);
          if (finishedCard) {
            finishedCard.style.opacity = "0";
            finishedCard.style.transform = "scale(0.95)";
            setTimeout(() => {
              if (finishedCard.parentNode) {
                finishedCard.remove();
              }
            }, 300);
          }
        }
      });
    }

    previousOrderIds = new Set(activeOrderIds);
  } catch (err) {
    console.warn("Error fetching order state:", err);
    setConnectionState("offline");
    if (isInitialLoad) {
      loadingState.classList.add("hidden");
      emptyState.classList.remove("hidden");
      isInitialLoad = false;
    }
  }
}

// Initial fetch and polling schedule
fetchState();
setInterval(fetchState, POLL_INTERVAL_MS);
