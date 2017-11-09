var asString = function (data) {
    return JSON.stringify(data, null, 4);
};

var getJson = function (e) {

    // inform the user we're trying
    var loading = asString({ "status": "loading" });
    $('#data').text(data);

    // get user input & validate
    var url = $('#url').val();
    var stream = $('#streamname').val();
    var time = $('#time').val();

    // convert the time value from a datetime to the number of minutes that have passed
    var diff = new Date() - new Date(time);
    var duration = Math.floor((diff / 1000) / 60);

    // get data
    url = new URL(url, "http://localhost:4000");
    url.searchParams.append("streamname", stream);
    url.searchParams.append("duration", time);
    
    $.getJSON(url)
        .done(function (data) {
            data = asString(data);
            $('#data').text(data);
        })
        .fail(function (res, error) {
            var errorText = res.statusText;
            if (errorText === "error") {
                errorText = "could not connect to server";
            }
            data = asString({ "status": errorText });
            $('#data').text(data);
        })
        .always(function () {
            $('#data').removeClass("prettyprinted");
            PR.prettyPrint();
        });
};

var setDefaultTime = function () {
    var tenMinutesInMilliseconds = 1000 * 60 * 10;
    var tenMinsAgo = new Date(new Date() - tenMinutesInMilliseconds)
        .toISOString()
        .slice(0, -5);
    console.log(tenMinsAgo);
    document.getElementById('time').value = tenMinsAgo;
    document.getElementById('time').defaultValue = tenMinsAgo;
};

$(function () {
    // setDefaultTime();

    $('#time').datetimepicker({
        locale: 'pt-br',
        maxDate: moment().subtract("minutes", 10)
      });
});