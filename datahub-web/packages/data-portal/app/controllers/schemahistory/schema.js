import Controller, { inject } from '@ember/controller';
import { computed } from '@ember/object';

export default Controller.extend({
  schemahistory: inject,
  actions: {
    onSelect: function(dataset, data) {
      const schemaHistoryController = this.schemahistory;

      schemaHistoryController.highlightRow(dataset, data, false);
      if (dataset && dataset.id != 0) {
        schemaHistoryController.updateTimeLine(dataset.id, false);
      }
    }
  },
  previousPage: computed('model.data.page', function() {
    var model = this.get('model');
    if (model && model.data && model.data.page) {
      var currentPage = model.data.page;
      if (currentPage <= 1) {
        return currentPage;
      } else {
        return currentPage - 1;
      }
    } else {
      return 1;
    }
  }),
  nextPage: computed('model.data.page', function() {
    var model = this.get('model');
    if (model && model.data && model.data.page) {
      var currentPage = model.data.page;
      var totalPages = model.data.totalPages;
      if (currentPage >= totalPages) {
        return totalPages;
      } else {
        return currentPage + 1;
      }
    } else {
      return 1;
    }
  }),
  first: computed('model.data.page', function() {
    var model = this.get('model');
    if (model && model.data && model.data.page) {
      var currentPage = model.data.page;
      if (currentPage <= 1) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }),
  last: computed('model.data.page', function() {
    var model = this.get('model');
    if (model && model.data && model.data.page) {
      var currentPage = model.data.page;
      var totalPages = model.data.totalPages;
      if (currentPage >= totalPages) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  })
});
