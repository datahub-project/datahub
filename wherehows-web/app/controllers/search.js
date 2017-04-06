import Ember from 'ember';

const {
  Controller,
  computed,
  get,
  set,
  String: {capitalize}
} = Ember;

const sources = [
  'all',
  'dali',
  'espresso',
  'hive',
  'hdfs',
  'kafka',
  'oracle',
  'teradata',
  'voldemort'
];

export default Controller.extend({
  queryParams: [
    'keyword',
    'category',
    'source',
    'page'
  ],
  keyword: '',
  category: 'datasets',
  source: 'all',
  page: 1,

  sources: computed('source', function () {
    return sources.map(source => ({
      name: 'source',
      value: source,
      label: capitalize(source),
      group: String(get(this, 'source')).toLowerCase()
    }))
  }),

  isMetric: false,

  datasetTitle: function () {
    var model = this.get("model");
    if (model && model.source) {
      if (model.source.toLocaleLowerCase() != 'all') {
        return model.source;
      }
    }
    return "Datasets";
  }.property('model.source'),
  isDatasets: function () {
    var model = this.get("model");
    if (model && model.category) {
      if (model.category.toLocaleLowerCase() === 'datasets') {
        return true;
      }
    }
    return false;
  }.property('model.category'),
  isComments: function () {
    var model = this.get("model");
    if (model && model.category) {
      if (model.category.toLocaleLowerCase() === 'comments') {
        return true;
      }
    }
    return false;
  }.property('model.category'),
  isMetrics: function () {
    var model = this.get("model");
    if (model && model.category) {
      if (model.category.toLocaleLowerCase() === 'metrics') {
        return true;
      }
    }
    return false;
  }.property('model.category'),
  isFlows: function () {
    var model = this.get("model");
    if (model && model.category) {
      if (model.category.toLocaleLowerCase() === 'flows') {
        return true;
      }
    }
    return false;
  }.property('model.category'),
  isJobs: function () {
    var model = this.get("model");
    if (model && model.category) {
      if (model.category.toLocaleLowerCase() === 'jobs') {
        return true;
      }
    }
    return false;
  }.property('model.category'),
  previousPage: function () {
    var model = this.get("model");
    if (model && model.page) {
      var currentPage = model.page;
      if (currentPage <= 1) {
        return currentPage;
      }
      else {
        return currentPage - 1;
      }
    } else {
      return 1;
    }

  }.property('model.page'),
  nextPage: function () {
    var model = this.get("model");
    if (model && model.page) {
      var currentPage = model.page;
      var totalPages = model.totalPages;
      if (currentPage >= totalPages) {
        return totalPages;
      }
      else {
        return currentPage + 1;
      }
    } else {
      return 1;
    }
  }.property('model.page'),
  first: function () {
    var model = this.get("model");
    if (model && model.page) {
      var currentPage = model.page;
      if (currentPage <= 1) {
        return true;
      }
      else {
        return false
      }
    } else {
      return false;
    }
  }.property('model.page'),
  last: function () {
    var model = this.get("model");
    if (model && model.page) {
      var currentPage = model.page;
      var totalPages = model.totalPages;
      if (currentPage >= totalPages) {
        return true;
      }
      else {
        return false
      }
    } else {
      return false;
    }
  }.property('model.page'),

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
