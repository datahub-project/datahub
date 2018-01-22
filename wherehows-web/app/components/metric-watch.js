import Controller from '@ember/controller';
import $ from 'jquery';

export default Component.extend({
  actions: {
    watch: function(metric) {
      var url = '/api/v1/metrics/' + metric.id + '/watch';
      var method = !metric.watchId ? 'POST' : 'DELETE';
      if (method.toLowerCase() === 'delete') url += '/' + metric.watchId;
      var token = $('#csrfToken')
        .val()
        .replace('/', '');
      var _this = this;
      $.ajax({
        url: url,
        method: method,
        headers: {
          'Csrf-Token': token
        },
        dataType: 'json',
        data: {
          csrfToken: token,
          item_type: 'metric',
          notification_type: 'weekly'
        }
      })
        .done(function(data, txt, xhr) {
          _this.set('metric.watchId', data.watchId);
          // _this.sendAction('getMetrics')
        })
        .fail(function(xhr, txt, err) {
          console.log('Error: Could not watch metric.');
        });
    }
  }
});
