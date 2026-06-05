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
  $( "#result" ).empty().append( displayMessage );
}
