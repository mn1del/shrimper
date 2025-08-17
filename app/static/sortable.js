// Enable sorting for tables with the "sortable" class. Clicking a header
// sorts by that column; clicking again reverses the order. Simple arrow icons
// are added to indicate sortability and the current direction.

// Comparison helper that ensures empty strings always sort last.
function compareValues(textA, textB, direction) {
  const isEmptyA = textA === '';
  const isEmptyB = textB === '';
  if (isEmptyA && isEmptyB) return 0;
  if (isEmptyA) return 1; // A should always go to the bottom
  if (isEmptyB) return -1; // B should always go to the bottom

  const compare = textA.localeCompare(textB, undefined, { numeric: true });
  return direction === 'asc' ? compare : -compare;
}

if (typeof document !== 'undefined') {
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
            return compareValues(textA, textB, direction);
          });
          rows.forEach(row => tbody.appendChild(row));
        });
      });

      const defaultHeader = table.querySelector('th[data-default-sort]');
      if (defaultHeader) {
        const direction = defaultHeader.getAttribute('data-default-sort');
        const index = Array.from(headers).indexOf(defaultHeader);
        const icon = defaultHeader.querySelector('.sort-icon');
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));

        defaultHeader.setAttribute('data-sort', direction);
        if (icon) {
          icon.textContent = direction === 'asc' ? '▲' : '▼';
        }

        rows.sort(function (a, b) {
          const textA = a.children[index].innerText.trim();
          const textB = b.children[index].innerText.trim();
          return compareValues(textA, textB, direction);
        });
        rows.forEach(row => tbody.appendChild(row));
      }
    });
  });
}

// Export for testing in Node environments
if (typeof module !== 'undefined') {
  module.exports = { compareValues };
}
