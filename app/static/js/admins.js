(function () {
  const state = {
    admins: Array.isArray(window.__INITIAL_ADMINS__) ? window.__INITIAL_ADMINS__ : [],
    currentId: Number(window.__CURRENT_ADMIN_ID__ || 0),
    table: document.getElementById('adminsTable'),
    alert: document.getElementById('adminAlert'),
    form: document.getElementById('addAdminForm'),
    refreshBtn: document.getElementById('refreshAdmins'),
  };

  function showMessage(type, text) {
    if (!state.alert) return;
    state.alert.className = `alert alert-${type}`;
    state.alert.textContent = text;
    state.alert.classList.remove('d-none');
    window.setTimeout(() => {
      state.alert.classList.add('d-none');
    }, 4000);
  }

  function renderAdmins() {
    if (!state.table) return;
    const body = state.table.querySelector('tbody');
    if (!body) return;
    body.innerHTML = '';
    state.admins.forEach((admin) => {
      const tr = document.createElement('tr');
      const isCurrent = Number(admin.id) === state.currentId;
      const statusBadge = admin.is_active ? 'success' : 'secondary';
      const statusText = admin.is_active ? 'Active' : 'Disabled';
      const roleBadge = admin.is_superadmin ? 'warning' : 'info';
      const roleText = admin.is_superadmin ? 'Super admin' : 'Admin';
      const lastLogin = admin.last_login_at ? admin.last_login_at : '—';

      tr.innerHTML = `
        <td>${admin.email}</td>
        <td><span class="badge text-bg-${statusBadge}">${statusText}</span></td>
        <td><span class="badge text-bg-${roleBadge}">${roleText}</span></td>
        <td>${lastLogin}</td>
        <td class="text-end">
          <div class="btn-group" role="group">
            <button type="button" class="btn btn-sm btn-outline-primary" data-action="reset" data-id="${admin.id}">Send reset</button>
            <button type="button" class="btn btn-sm btn-outline-${admin.is_active ? 'secondary' : 'success'}" data-action="toggle-active" data-id="${admin.id}" ${isCurrent ? 'disabled' : ''}>
              ${admin.is_active ? 'Disable' : 'Enable'}
            </button>
            <button type="button" class="btn btn-sm btn-outline-${admin.is_superadmin ? 'danger' : 'success'}" data-action="toggle-role" data-id="${admin.id}" ${isCurrent && admin.is_superadmin ? 'disabled' : ''}>
              ${admin.is_superadmin ? 'Demote' : 'Promote'}
            </button>
          </div>
        </td>
      `;
      body.appendChild(tr);
    });
  }

  async function loadAdmins() {
    try {
      const res = await fetch('/api/admins', { cache: 'no-store' });
      if (!res.ok) throw new Error('Failed to load admins');
      const data = await res.json();
      state.admins = Array.isArray(data.admins) ? data.admins : [];
      renderAdmins();
    } catch (err) {
      showMessage('danger', err.message || 'Unable to load admins');
    }
  }

  async function createAdmin(formData) {
    const payload = {
      email: formData.get('email') || '',
      password: formData.get('password') || '',
      is_superadmin: formData.get('is_superadmin') === 'on',
    };
    try {
      const res = await fetch('/api/admins', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed to create admin');
      state.admins.push(data.admin);
      renderAdmins();
      showMessage('success', 'Admin created');
      state.form.reset();
    } catch (err) {
      showMessage('danger', err.message || 'Unable to create admin');
    }
  }

  async function updateAdmin(adminId, payload) {
    try {
      const res = await fetch(`/api/admins/${adminId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Update failed');
      const idx = state.admins.findIndex((a) => Number(a.id) === Number(adminId));
      if (idx >= 0) {
        state.admins[idx] = data.admin;
        renderAdmins();
      }
      showMessage('success', 'Admin updated');
    } catch (err) {
      showMessage('danger', err.message || 'Unable to update admin');
    }
  }

  async function sendReset(adminId) {
    try {
      const res = await fetch(`/api/admins/${adminId}/reset`, { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Unable to send reset email');
      showMessage('info', data.sent ? 'Reset link sent.' : 'Reset email suppressed.');
    } catch (err) {
      showMessage('danger', err.message || 'Unable to send reset email');
    }
  }

  function handleTableClick(event) {
    const target = event.target.closest('button[data-action]');
    if (!target) return;
    const adminId = target.getAttribute('data-id');
    const action = target.getAttribute('data-action');
    if (!adminId || !action) return;
    if (action === 'toggle-active') {
      const admin = state.admins.find((a) => Number(a.id) === Number(adminId));
      if (!admin) return;
      updateAdmin(adminId, { is_active: !admin.is_active });
    } else if (action === 'toggle-role') {
      const admin = state.admins.find((a) => Number(a.id) === Number(adminId));
      if (!admin) return;
      updateAdmin(adminId, { is_superadmin: !admin.is_superadmin });
    } else if (action === 'reset') {
      sendReset(adminId);
    }
  }

  function init() {
    renderAdmins();
    if (state.form) {
      state.form.addEventListener('submit', (event) => {
        event.preventDefault();
        createAdmin(new FormData(state.form));
      });
    }
    if (state.table) {
      state.table.addEventListener('click', handleTableClick);
    }
    if (state.refreshBtn) {
      state.refreshBtn.addEventListener('click', loadAdmins);
    }
  }

  document.addEventListener('DOMContentLoaded', init);
})();
