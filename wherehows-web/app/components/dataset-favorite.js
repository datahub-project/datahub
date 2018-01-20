import Component from '@ember/component';
import $ from 'jquery';

export default Component.extend({
  actions: {
    favorites: function(dataset) {
      var url = '/api/v1/datasets/' + dataset.id + '/favorite';
      var method = !dataset.isFavorite ? 'POST' : 'DELETE';
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
          _this.set('dataset.isFavorite', !dataset.isFavorite);
        })
        .fail(function(xhr, txt, err) {
          console.log('Error: Could not update dataset favorite.');
        });
    }
  }
});
