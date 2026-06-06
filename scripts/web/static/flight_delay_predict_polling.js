// Render the response on the page for splits:
// [-float("inf"), -15.0, 0, 30.0, float("inf")]
function renderPage(response) {
  var displayMessage;
  if(response.Prediction == 0 || response.Prediction == '0') {
    displayMessage = "Early (15+ Minutes Early)";
  } else if(response.Prediction == 1 || response.Prediction == '1') {
    displayMessage = "Slightly Early (0-15 Minute Early)";
  } else if(response.Prediction == 2 || response.Prediction == '2') {
    displayMessage = "Slightly Late (0-30 Minute Delay)";
  } else if(response.Prediction == 3 || response.Prediction == '3') {
    displayMessage = "Very Late (30+ Minutes Late)";
  }
  var list = document.getElementById('result-list');
  if (!list) return;
  var proc = list.querySelector('.proc');
  if (proc) proc.remove();
  fetch('/api/models').then(function(r) { return r.json(); }).then(function(data) {
    var active = data.models ? data.models.find(function(m) { return m.run_id === data.active_run_id; }) : null;
    var modelName = active ? (active.run_name || active.run_id.slice(0,8)) : '—';
    var acc = active && active.metrics && active.metrics.accuracy !== undefined
      ? (active.metrics.accuracy * 100).toFixed(1) + '%' : '—';
    var card = document.createElement('div');
    card.style.cssText = 'padding:0.4rem 0;border-bottom:1px solid #2a2a3a';
    card.innerHTML = '<div style="display:flex;justify-content:space-between;align-items:center">'
      + '<span style="font-size:0.85rem;font-weight:600">' + displayMessage + '</span>'
      + '<span style="font-size:0.6rem;color:#555">' + modelName + ' ' + acc + '</span>'
      + '</div>';
    list.insertBefore(card, list.firstChild);
  });
}
