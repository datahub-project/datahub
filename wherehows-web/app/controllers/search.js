import Controller from '@ember/controller';
import { computed, set, get } from '@ember/object';
import { capitalize } from '@ember/string';

const sources = ['all', 'dali', 'espresso', 'hive', 'hdfs', 'kafka', 'oracle', 'teradata', 'voldemort'];

export default Controller.extend({
  queryParams: ['keyword', 'category', 'source', 'page'],
  keyword: '',
  category: 'datasets',
  source: 'all',
  page: 1,
  header: 'Refine By',

  sources: computed('source', function() {
    return sources.map(source => ({
      name: 'source',
      value: source,
      label: capitalize(source),
      group: String(get(this, 'source')).toLowerCase()
    }));
  }),

  isMetric: false,

  datasetTitle: computed('model.source', function() {
    var model = this.get('model');
    if (model && model.source) {
      if (model.source.toLocaleLowerCase() != 'all') {
        return model.source;
      }
    }
    return 'Datasets';
  }),
  isDatasets: computed('model.category', function() {
    var model = this.get('model');
    if (model && model.category) {
      if (model.category.toLocaleLowerCase() === 'datasets') {
        return true;
      }
    }
    return false;
  }),
  isComments: computed('model.category', function() {
    var model = this.get('model');
    if (model && model.category) {
      if (model.category.toLocaleLowerCase() === 'comments') {
        return true;
      }
    }
    return false;
  }),
  isMetrics: computed('model.category', function() {
    var model = this.get('model');
    if (model && model.category) {
      if (model.category.toLocaleLowerCase() === 'metrics') {
        return true;
      }
    }
    return false;
  }),
  isFlows: computed('model.category', function() {
    var model = this.get('model');
    if (model && model.category) {
      if (model.category.toLocaleLowerCase() === 'flows') {
        return true;
      }
    }
    return false;
  }),
  isJobs: computed('model.category', function() {
    var model = this.get('model');
    if (model && model.category) {
      if (model.category.toLocaleLowerCase() === 'jobs') {
        return true;
      }
    }
    return false;
  }),
  previousPage: computed('model.page', function() {
    var model = this.get('model');
    if (model && model.page) {
      var currentPage = model.page;
      if (currentPage <= 1) {
        return currentPage;
      } else {
        return currentPage - 1;
      }
    } else {
      return 1;
    }
  }),
  nextPage: computed('model.page', function() {
    var model = this.get('model');
    if (model && model.page) {
      var currentPage = model.page;
      var totalPages = model.totalPages;
      if (currentPage >= totalPages) {
        return totalPages;
      } else {
        return currentPage + 1;
      }
    } else {
      return 1;
    }
  }),
  first: computed('model.page', function() {
    var model = this.get('model');
    if (model && model.page) {
      var currentPage = model.page;
      if (currentPage <= 1) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }),
  last: computed('model.page', function() {
    var model = this.get('model');
    if (model && model.page) {
      var currentPage = model.page;
      var totalPages = model.totalPages;
      if (currentPage >= totalPages) {
        return true;
      } else {
        return false;
      }
    } else {
      return false;
    }
  }),

  actions: {
    sourceDidChange(groupName, value) {
      set(this, groupName, value);
    },

    startDateDidChange(date = null) {
      set(this, 'startDate', date);
    },

    endDateDidChange(date = null) {
      set(this, 'endDate', date);
    }
  }
});
