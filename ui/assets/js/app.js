
/**
 * Insurance Claim Management - Frontend SPA
 * Uses jQuery for all data interactions.
 *
 * By default this app uses a mock API (localStorage)
 * so it is fully functional without a backend.
 *
 * To integrate with a real backend, replace the functions in mockApi
 * with real $.ajax() calls to your REST endpoints.
 */

const STORAGE_KEY = "insuranceClaims";
let claimsCache = [];
let statusChart = null;
let trendChart = null;

// ---------- Mock API (can be replaced with real AJAX calls) ---------- //

// Helper: switch to real API by wrapping $.ajax
// Example (not used by default):
 const api = {
   getClaims: () => $.ajax({ url: "http://localhost:8000/api/claims", method: "GET" }),
   createClaim: (data) => $.ajax({ url: "http://localhost:8000/api/claims", method: "POST", data: JSON.stringify(data), contentType: "application/json" }),
   updateClaim: (id, data) => $.ajax({ url: `http://localhost:8000/api/claims/${id}`, method: "PUT", data: JSON.stringify(data), contentType: "application/json" }),
   deleteClaim: (id) => $.ajax({ url: `http://localhost:8000/api/claims/${id}`, method: "DELETE" }),
   getPolicies: () => $.ajax({ url: "http://localhost:8000/api/policy", method: "GET" })
 };
//
// For now we use mockApi:
// const api = "http://localhost:8000";

// ---------- Local Storage Helpers ---------- //

function loadPolicyDropdown() {
  api.getPolicies()
    .done(policies => {
      const dropdown = $("#policyNumber");
      dropdown.empty();
      dropdown.append(`<option value="">-- Select Policy Number --</option>`);

      policies.forEach(p => {
        dropdown.append(`<option value="${p.policy_number}">${p.policy_number}</option>`);
      });
    })
    .fail((xhr) => {
        console.error("Failed to load policy numbers", xhr.status, xhr.responseText);
    });
}



function loadClaimsFromStorage() {
  const raw = localStorage.getItem(STORAGE_KEY);
  if (raw) {
    try {
      claimsCache = JSON.parse(raw);
    } catch (e) {
      console.error("Failed to parse stored claims", e);
      claimsCache = [];
    }
  } else {
    // Seed sample data on first load
    if (!claimsCache.length) {
      claimsCache = seedSampleClaims();
      saveClaimsToStorage();
    }
  }
}

function saveClaimsToStorage() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(claimsCache));
}

function seedSampleClaims() {
  const today = new Date();
  function daysAgo(n) {
    const d = new Date();
    d.setDate(today.getDate() - n);
    return d;
  }

  return [
    {
      id: 1,
      policy_id: "POL-1001",
      amount: 50000,
      status: "Approved",
      incidentDate: daysAgo(45).toISOString().substring(0, 10),
      description: "Hospitalisation due to surgery",
      createdAt: daysAgo(44).toISOString()
    },
    {
      id: 2,
      policy_id: "POL-1002",
      amount: 120000,
      status: "Pending",
      incidentDate: daysAgo(12).toISOString().substring(0, 10),
      description: "Rear-end collision damages",
      createdAt: daysAgo(11).toISOString()
    },
    {
      id: 3,
      policy_id: "POL-1003",
      amount: 250000,
      status: "Rejected",
      incidentDate: daysAgo(70).toISOString().substring(0, 10),
      description: "Water damage to property",
      createdAt: daysAgo(69).toISOString()
    },
    {
      id: 4,
      policy_id: "POL-1004",
      amount: 750000,
      status: "Approved",
      incidentDate: daysAgo(5).toISOString().substring(0, 10),
      description: "Critical illness claim",
      createdAt: daysAgo(4).toISOString()
    },
    {
      id: 5,
      policy_id: "POL-1005",
      amount: 90000,
      status: "Pending",
      incidentDate: daysAgo(2).toISOString().substring(0, 10),
      description: "Minor accident claim",
      createdAt: daysAgo(1).toISOString()
    }
  ];
}

// ---------- UI Helpers ---------- //

function switchSection(targetSelector) {
  $(".section-content").addClass("d-none");
  $(targetSelector).removeClass("d-none");

  $(".main-nav-link").removeClass("active");
  $(`.main-nav-link[data-target="${targetSelector}"]`).addClass("active");
}

function formatCurrency(amount) {
  if (amount == null || isNaN(amount)) return "0";
  return amount.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatDateTime(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  if (isNaN(d.getTime())) return "-";
  return d.toLocaleString();
}

// ---------- Metrics & Charts ---------- //

function computeMetrics(claims) {
  const metrics = {
    total: claims.length,
    pending: 0,
    approved: 0,
    rejected: 0,
    totalAmount: 0
  };

  const statusMap = {
    "Pending": "pending",
    "Approved": "approved",
    "Rejected": "rejected"
  };

  claims.forEach(c => {
    const key = statusMap[c.status] || null;
    if (key) metrics[key]++;
    if (typeof c.amount === "number") metrics.totalAmount += c.amount;
  });

  return metrics;
}

function computeTrend(claims) {
  // Last 6 months including current
  const labels = [];
  const now = new Date();
  const counts = [];

  for (let i = 5; i >= 0; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const label = d.toLocaleString("default", { month: "short", year: "2-digit" });
    labels.push(label);
    counts.push(0);
  }

  claims.forEach(c => {
    const created = new Date(c.createdAt || c.incidentDate);
    if (isNaN(created.getTime())) return;

    const monthsDiff =
      (now.getFullYear() - created.getFullYear()) * 12 +
      (now.getMonth() - created.getMonth());

    const index = 5 - monthsDiff;
    if (monthsDiff >= 0 && monthsDiff <= 5 && index >= 0 && index < counts.length) {
      counts[index]++;
    }
  });

  return { labels, counts };
}

function updateMetricsAndCharts(claims) {
  const metrics = computeMetrics(claims);

  $("#metricTotalClaims").text(metrics.total);
  $("#metricPendingClaims").text(metrics.pending);
  $("#metricApprovedClaims").text(metrics.approved);
  $("#metricRejectedClaims").text(metrics.rejected);

  // Status chart
  const statusData = [metrics.pending, metrics.approved, metrics.rejected];
  const statusLabels = ["Pending", "Approved", "Rejected"];

  if (!statusChart) {
    const ctxStatus = document.getElementById("statusChart");
    if (ctxStatus) {
      statusChart = new Chart(ctxStatus, {
        type: "doughnut",
        data: {
          labels: statusLabels,
          datasets: [{
            label: "Claims by Status",
            data: statusData
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { position: "bottom" }
          }
        }
      });
    }
  } else {
    statusChart.data.datasets[0].data = statusData;
    statusChart.update();
  }

  // Trend chart
  const trend = computeTrend(claims);
  if (!trendChart) {
    const ctxTrend = document.getElementById("trendChart");
    if (ctxTrend) {
      trendChart = new Chart(ctxTrend, {
        type: "line",
        data: {
          labels: trend.labels,
          datasets: [{
            label: "Claims Created",
            data: trend.counts,
            tension: 0.3
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            y: {
              beginAtZero: true,
              precision: 0
            }
          }
        }
      });
    }
  } else {
    trendChart.data.labels = trend.labels;
    trendChart.data.datasets[0].data = trend.counts;
    trendChart.update();
  }
}

// ---------- Table Rendering ---------- //

function renderClaimsTable(claims) {
  const tbody = $("#claimsTableBody");
  tbody.empty();

  if (!claims.length) {
    tbody.append(
      `<tr><td colspan="8" class="text-center text-muted">No claims found.</td></tr>`
    );
    $("#viewClaimsCounter").text("0 claim(s) found");
    return;
  }

  claims.forEach(c => {
    const row = `
      <tr>
        <td>${c.claim_id}</td>
        <td>${c.policy_id}</td>
        <td>${c.status}</td>
        <td>₹${formatCurrency(c.claim_amount)}</td>
        <td>
          <span class="badge bg-${
            c.status === "Approved"
              ? "success"
              : c.status === "Rejected"
              ? "danger"
              : "warning text-dark"
          }">${c.status}</span>
        </td>
        <td>${c.claim_date || "-"}</td>
        <td>${formatDateTime(c.createdAt)}</td>
      </tr>
    `;
    tbody.append(row);
  });

  $("#viewClaimsCounter").text(`${claims.length} claim(s) found`);
}

function renderDeleteTable(claims) {
  const tbody = $("#deleteClaimsTableBody");
  tbody.empty();

  if (!claims.length) {
    tbody.append(
      `<tr><td colspan="6" class="text-center text-muted">No claims available to delete.</td></tr>`
    );
    return;
  }

  claims.forEach(c => {
    const row = `
      <tr>
        <td>${c.id}</td>
        <td>${c.policy_id}</td>
        <td>${c.status}</td>
        <td>₹${formatCurrency(c.amount)}</td>
        <td>
          <button class="btn btn-sm btn-outline-danger btn-delete-claim" data-id="${c.id}">
            Delete
          </button>
        </td>
      </tr>
    `;
    tbody.append(row);
  });
}

function populateUpdateSelect(claims) {
  const select = $("#updateClaimId");
  select.empty();
  if (!claims.length) {
    select.append('<option value="">No claims available</option>');
    $("#updateClaimForm").addClass("d-none");
    return;
  }
  select.append('<option value="">Select a claim...</option>');
  claims.forEach(c => {
    select.append(`<option value="${c.id}">#${c.id} - ${c.policy_id}</option>`);
  });
}

// ---------- Filtering & Searching ---------- //

function applyViewFilters() {
  const statusFilter = $("#filterStatus").val();
  const search = $("#searchText").val().toLowerCase();

  let filtered = claimsCache.slice();

  if (statusFilter && statusFilter !== "ALL") {
    filtered = filtered.filter(c => c.status === statusFilter);
  }

  if (search) {
    filtered = filtered.filter(c =>
      String(c.id).includes(search) ||
      (c.policy_id && c.policy_id.toLowerCase().includes(search))
    );
  }

  renderClaimsTable(filtered);
}

// ---------- Form Handlers ---------- //

function handleNewClaimSubmit(e) {
  e.preventDefault();
  $("#newClaimSuccess").addClass("d-none");
  $("#newClaimError").addClass("d-none");

  const claimData = {
    policy_id: parseInt($("#policyNumber").val() || "0"),
    //customerName: $("#customerName").val().trim(),
    //claimType: $("#claimType").val(),
    claim_amount: parseFloat($("#claimAmount").val() || "0"),
    status: $("#claimStatus").val(),
    claim_date: $("#incidentDate").val(),
    description: $("#description").val().trim()
  };

  api.createClaim(claimData)
    .done(claim => {
      loadClaimsFromStorage();
      updateMetricsAndCharts(claimsCache);
      applyViewFilters();
      renderDeleteTable(claimsCache);
      populateUpdateSelect(claimsCache);
      $("#newClaimForm")[0].reset();
      $("#newClaimSuccess").removeClass("d-none");
    })
    .fail(() => {
      $("#newClaimError").removeClass("d-none");
    });
}

function handleUpdateClaimChange() {
  $("#updateClaimSuccess").addClass("d-none");
  $("#updateClaimError").addClass("d-none");
  const id = parseInt($("#updateClaimId").val(), 10);
  if (!id) {
    $("#updateClaimForm").addClass("d-none");
    return;
  }
  const claim = claimsCache.find(c => c.id === id);
  if (!claim) {
    $("#updateClaimForm").addClass("d-none");
    return;
  }
  $("#updatePolicyNumber").val(claim.policy_id);
  //$("#updateCustomerName").val(claim.customerName);
  //$("#updateClaimType").val(claim.claimType);
  $("#updateClaimAmount").val(claim.amount);
  $("#updateClaimStatus").val(claim.status);
  $("#updateIncidentDate").val(claim.incidentDate);
  $("#updateDescription").val(claim.description);
  $("#updateClaimForm").removeClass("d-none");
}

function handleUpdateClaimSubmit(e) {
  e.preventDefault();
  $("#updateClaimSuccess").addClass("d-none");
  $("#updateClaimError").addClass("d-none");

  const id = parseInt($("#updateClaimId").val(), 10);
  if (!id) return;

  const updates = {
    policy_id: $("#updatePolicyNumber").val().trim(),
    //customerName: $("#updateCustomerName").val().trim(),
    //claimType: $("#updateClaimType").val(),
    amount: parseFloat($("#updateClaimAmount").val() || "0"),
    status: $("#updateClaimStatus").val(),
    incidentDate: $("#updateIncidentDate").val(),
    description: $("#updateDescription").val().trim()
  };

  api.updateClaim(id, updates)
    .done(() => {
      loadClaimsFromStorage();
      updateMetricsAndCharts(claimsCache);
      applyViewFilters();
      renderDeleteTable(claimsCache);
      populateUpdateSelect(claimsCache);
      $("#updateClaimSuccess").removeClass("d-none");
    })
    .fail(() => {
      $("#updateClaimError").removeClass("d-none");
    });
}

function handleDeleteClaimClick(e) {
  const btn = $(e.currentTarget);
  const id = parseInt(btn.data("id"), 10);
  $("#deleteClaimSuccess").addClass("d-none");
  $("#deleteClaimError").addClass("d-none");

  if (!confirm(`Are you sure you want to delete claim #${id}?`)) {
    return;
  }

  api.deleteClaim(id)
    .done(() => {
      loadClaimsFromStorage();
      updateMetricsAndCharts(claimsCache);
      applyViewFilters();
      renderDeleteTable(claimsCache);
      populateUpdateSelect(claimsCache);
      $("#deleteClaimSuccess").removeClass("d-none");
    })
    .fail(() => {
      $("#deleteClaimError").removeClass("d-none");
    });
}

// ---------- Initialization ---------- //

$(document).ready(function () {
  // Footer year
  $("#footerYear").text(new Date().getFullYear());

  // Nav handlers
  $(".main-nav-link").on("click", function (e) {
    e.preventDefault();
    const target = $(this).data("target");
    if (target) {
      switchSection(target);
    }
  });

  // Initial data load
  api.getClaims().done(claims => {
    claimsCache = claims;
    updateMetricsAndCharts(claimsCache);
    renderClaimsTable(claimsCache);
    renderDeleteTable(claimsCache);
    populateUpdateSelect(claimsCache);

    loadPolicyDropdown();
  });

  // New claim form
  $("#newClaimForm").on("submit", handleNewClaimSubmit);

  // Update claim form
  $("#updateClaimId").on("change", handleUpdateClaimChange);
  $("#updateClaimForm").on("submit", handleUpdateClaimSubmit);

  // Delete claim buttons (event delegation)
  $("#deleteClaimsTableBody").on("click", ".btn-delete-claim", handleDeleteClaimClick);

  // Filters
  $("#filterStatus").on("change", applyViewFilters);
  $("#searchText").on("input", applyViewFilters);
});
