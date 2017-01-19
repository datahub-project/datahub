App.AdvsearchController = Ember.Controller.extend({
  queryParams: ['query', 'page'],
  query: null,
  page: null,
  showNoResult: false,
  previousPage: function(){
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
  nextPage: function(){
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
  first: function(){
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
  last: function(){
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
    switchSearchToMetric: function(keyword){
      window.location = "#/search/" + keyword + '/metric/page/1';
    },
    switchSearchToComments: function(keyword){
      window.location = "#/search/" + keyword + '/comments/page/1';
    }
  }
});
