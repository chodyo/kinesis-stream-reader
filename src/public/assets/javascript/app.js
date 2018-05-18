var asString = function (data) {
    return JSON.stringify(data, null, 4);
};

var updateData = function (data) {
    // debugger;
    $('#data').text(data);
    $('#data').removeClass('prettyprinted');
    PR.prettyPrint();
};

var getJson = function (e) {

    // inform the user we're trying
    var loading = asString({ 'status': 'loading' });
    updateData(loading);

    // get user input & validate
    var url = $('#url').val() + 'records';
    var stream = $('#streamname').val();
    var time = $('#minutes').val();

    // get data
    url = new URL(url, 'http://localhost:4000/records');
    url.searchParams.append('streamname', stream);
    url.searchParams.append('duration', time);
    console.log(url);
    $.getJSON(url)
        .done(function (data) {
            data = asString(data);
            updateData(data);
        })
        .fail(function (res, error) {
            var errorText = res.statusText;
            if (errorText === 'error') {
                errorText = 'could not connect to server';
            }
            data = asString({ 'status': errorText });
            console.log('errored out', data);
            updateData(data);
        });
};

var setDefaultUrl = function () {
    var url = window.location.origin;
    $('#url').val(url);
    console.log(url);
};

$(function () {
    setDefaultUrl();
});