(function () {
  document.addEventListener('DOMContentLoaded', function () {
    var emailInput = document.querySelector('form input[type="email"]');
    if (emailInput && !emailInput.value) {
      emailInput.focus();
    }
  });
})();
