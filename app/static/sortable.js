document.addEventListener('DOMContentLoaded', function () {
  document.querySelectorAll('table.sortable').forEach(function (table) {
    const headers = table.querySelectorAll('th');
    headers.forEach(function (header, index) {
      header.style.cursor = 'pointer';
      header.addEventListener('click', function () {
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const current = header.getAttribute('data-sort') || 'asc';
        const direction = current === 'asc' ? 'desc' : 'asc';
        headers.forEach(h => h.removeAttribute('data-sort'));
        header.setAttribute('data-sort', direction);
        rows.sort(function (a, b) {
          const textA = a.children[index].innerText.trim();
          const textB = b.children[index].innerText.trim();
          const compare = textA.localeCompare(textB, undefined, {numeric: true});
          return direction === 'asc' ? compare : -compare;
        });
        rows.forEach(row => tbody.appendChild(row));
      });
    });
  });
});
