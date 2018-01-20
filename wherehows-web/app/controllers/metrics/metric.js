import Controller from '@ember/controller';
import $ from 'jquery';

export default Controller.extend({
  isEdit: false,
  updateLoading: false,
  lineageUrl: computed('model.refID', function() {
    var model = this.get('model');
    if (model) {
      if (model.refID) {
        var id = parseInt(model.refID);
        if (id > 0) {
          return '/lineage/metric/' + model.refID;
        }
      }
    }
    return '';
  }),
  showLineage: computed('model.refID', function() {
    var model = this.get('model');
    if (model) {
      if (model.refID) {
        var id = parseInt(model.refID);
        if (id > 0) {
          return true;
        }
      }
    }
    return false;
  }),
  actions: {
    editMode: function() {
      this.set('isEdit', true);
    },
    cancelEditMode: function() {
      this.set('isEdit', false);
    },
    update: function() {
      var model = this.get('model');
      var url = '/api/v1/metrics/' + model.id + '/update';
      var token = $('#csrfToken')
        .val()
        .replace('/', '');
      var _this = this;
      var data = JSON.parse(JSON.stringify(model));
      this.set('updateLoading', true);
      data.token = token;
      $.ajax({
        url: url,
        method: 'POST',
        //contentType: 'application/json',
        headers: {
          'Csrf-Token': token
        },
        dataType: 'json',
        //data: JSON.stringify(data)
        data: data
      })
        .done(function(data, txt, xhr) {
          _this.set('isEdit', false);
          _this.set('updateLoading', false);
        })
        .fail(function(xhr, txt, err) {
          Notify.toast('Could not update.', 'Update Metric', 'Error');
          _this.set('updateLoading', false);
        });
    }
  }
});
