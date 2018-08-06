import Controller from '@ember/controller';
import { scheduleOnce } from '@ember/runloop';
import { computed } from '@ember/object';
import $ from 'jquery';

let currentLeft;
let currentRight;
let leftSelected;
let rightSelected;
let chartData = [];
let schemaData = [];
let skipChangeEvent = false;

export default Controller.extend({
  schemaName: '',
  instance: jsondiffpatch.create({
    objectHash: function(obj, index) {
      if (typeof obj._id !== 'undefined') {
        return obj._id;
      }
      if (typeof obj.id !== 'undefined') {
        return obj.id;
      }
      if (typeof obj.name !== 'undefined') {
        return obj.name;
      }
      return '$$index:' + index;
    }
  }),
  actions: {
    onSelect: function(dataset, data) {
      this.highlightRow(dataset, data, false);
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
  }),

  updateSchemas(page, datasetId) {
    let url;
    if (!this.schemaName) {
      url = '/api/v1/schemaHistory/datasets?size=10&page=' + page;
    } else {
      url = '/api/v1/schemaHistory/datasets?name=' + this.schemaName + '&size=10&page=' + page;
    }

    if (datasetId && datasetId > 0) {
      url += '&datasetId=' + datasetId;
    }

    $.get(url, data => {
      if (data && data.status == 'ok') {
        this.set('model', data);
      }
    });
  },

  updateDiffView() {
    var delta = this.get('instance').diff(currentLeft, currentRight);
    $('#schemaContent').html(jsondiffpatch.formatters.html.format(delta, currentLeft));
    jsondiffpatch.formatters.html.hideUnchanged();
  },

  highlightRow(dataset, data, firstRow) {
    var rows = $('.schema-row');
    if (rows) {
      if (firstRow) {
        $(rows[0]).addClass('highlight');
        return;
      }
      for (var index = 0; index < data.data.datasets.length; index++) {
        if (dataset == data.data.datasets[index]) {
          $(rows[index])
            .addClass('highlight')
            .siblings()
            .removeClass('highlight');
          break;
        }
      }
    }
  }
});
