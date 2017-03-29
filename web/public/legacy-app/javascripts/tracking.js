(function(window, $){
    'use strict'

    var Tracking = window.InternalTracking = {};

    Tracking.track = function(trackingObj) {
        trackingObj.access_unixtime = new Date().getTime();
        var token = $("#csrfToken").val().replace('/', '')
        $.ajax({
            method: 'POST',
            url: '/api/v1/tracking',
            headers: {'Csrf-Token': token},
            dataType: 'json',
            contentType: 'application/json',
            data: JSON.stringify(trackingObj)
        }).done(function(data, txt, xhr) {
        }).fail(function(xhr, txt, err) {
            console.error('Could not track object', err)
        })
    }
})(window, jQuery);
