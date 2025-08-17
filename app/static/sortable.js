// Enable sorting for tables with the "sortable" class. Clicking a header
// sorts by that column; clicking again reverses the order. Simple arrow icons
// are added to indicate sortability and the current direction.
document.addEventListener('DOMContentLoaded', function () {
  document.querySelectorAll('table.sortable').forEach(function (table) {
    const headers = table.querySelectorAll('th');
    headers.forEach(function (header, index) {
      header.style.cursor = 'pointer';

      // Add sort indicator icon
      const icon = document.createElement('span');
      icon.className = 'ms-1 sort-icon';
      icon.textContent = '↕';
      header.appendChild(icon);

      header.addEventListener('click', function () {
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const current = header.getAttribute('data-sort');
        const direction = current === 'asc' ? 'desc' : 'asc';

        headers.forEach(h => {
          h.removeAttribute('data-sort');
          const ic = h.querySelector('.sort-icon');
          if (ic) {
            ic.textContent = '↕';
          }
        });

        header.setAttribute('data-sort', direction);
        icon.textContent = direction === 'asc' ? '▲' : '▼';

        rows.sort(function (a, b) {
          const textA = a.children[index].innerText.trim();
          const textB = b.children[index].innerText.trim();
          const compare = textA.localeCompare(textB, undefined, { numeric: true });
          return direction === 'asc' ? compare : -compare;
        });
        rows.forEach(row => tbody.appendChild(row));
      });
    });
  });
});
