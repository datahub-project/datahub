import Component from '@ember/component';
import $ from 'jquery';

export default Component.extend({
  actions: {
    watch: function(dataset) {
      var url = '/api/v1/datasets/' + dataset.id + '/watch';
      var method = !dataset.watchId ? 'POST' : 'DELETE';
      if (method.toLowerCase() === 'delete') url += '/' + dataset.watchId;
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
          item_type: 'dataset',
          notification_type: 'weekly'
        }
      })
        .done(function(data, txt, xhr) {
          _this.set('dataset.isWatched', !dataset.isWatched);
          _this.sendAction('getDatasets');
        })
        .fail(function(xhr, txt, err) {
          console.log('Error: Could not watch dataset.');
        });
    }
  }
});
