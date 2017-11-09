$(function () {

    $.getJSON('http://localhost:4000/records/?streamname=dev-agent-dl-events&duration=10')
        .done(function (data) {
            data = asString(data);
            $('#data').text(data);
        })
        .fail(function (res, error) {
            data = asString({ "status": res.statusText });
            $('#data').text(data);
        })
        .always(function () {
            $('#data').removeClass("prettyprinted");
            PR.prettyPrint();
        });
});

var asString = function (data) {
    return JSON.stringify(data, null, 4);
};