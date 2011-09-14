/* -*- JavaScript -*-
# The leading '#'s are just for consistency with other text formats
#
# rdf:
# dc:title Itinerary weather forecasts
# dc:date 2011-09-13
# dc:creator http://norman.walsh.name/knows/who#norman-walsh
*/

var today = new Date();
var daysec = 24*3600*1000;
var forecastURI = "http://www.wunderground.com/cgi-bin/findweather/hdfForecast";

$(document).ready(function() {
    checkForecasts();
});

function checkForecasts() {
    $("span.forecast").each(function(){forecast(this)});
}

function forecast(span) {
    var content = $(span).html().split(" ");
    var lat = content[0];
    var lon = content[1];
    var date = new Date(content[2]);

    days = Math.ceil((date.getTime() - today.getTime()) / daysec);

    console.log(days + " " + lat + " " + lon);

    if (days >= 0 && days <= 7) {
        var uri = "http://localhost:8403/forecast.xqy?callback=?";
        var params = { "query": lat + "," + lon, "date": content[2] };
        jQuery.getJSON(uri, params, function(data) {
            var text = "<a href="+forecastURI+"?query="+lat+"%2C"+lon+">";
            text += "Forecast"
            text += "</a>: ";
            text += data["conditions"];
            if (data["pop"] > 0) {
                text += " (" + data["pop"] + "%)";
            }
            text += "; high " + data["highf"] + "\u00B0F, low " + data["lowf"] + "\u00B0F.";
            $(span).html("<div>" + text + "</div>");
            $(span).css("display", "block");
        });
    }
}
