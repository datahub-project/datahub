import Component from '@ember/component';
import $ from 'jquery';

export default Component.extend({
  actions: {
    owned: function(dataset) {
      var url = '/api/v1/datasets/' + dataset.id + '/own';
      var method = !dataset.isOwned ? 'POST' : 'DELETE';
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
          csrfToken: token
        }
      })
        .done(function(data, txt, xhr) {
          if (data.status == 'success') {
            _this.set('dataset.isOwned', !dataset.isOwned);
            _this.set('dataset.owners', data.owners);
          }
        })
        .fail(function(xhr, txt, err) {
          console.log('Error: Could not update dataset owner.');
        });
    }
  }
});
