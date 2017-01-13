function highlightResultsforAdvSearch(result, index) {
  var content = result[index].schema;
  var len = content.length;
  if (len > 500) {
    content = content.substring(0, 500);
  }
  result[index].schema = content;
};

App.AdvsearchRoute = Ember.Route.extend({
  queryParams: {
    page: {
      refreshModel: true
    },
    query: {
      refreshModel: true
    }
  },
  model: function(params) {
    let query;
    var advsearchController = this.controllerFor('advsearch')
    advsearchController.set('loading', true)
    var q = convertQueryStringToObject()
    currentTab = 'Datasets';
    updateActiveTab();
    query = encodeURIComponent(atob(q.query));
    var url = 'api/v1/advsearch/search?searchOpts=' + query + '&page=' + params.page;
    $.get(url, function(data) {
      if (data && data.status == "ok") {
        for(var index = 0; index < data.result.data.length; index++) {
          var schema = data.result.data[index].schema;
          if (schema) {
            data.result.data[index].originalSchema = schema;
            highlightResultsforAdvSearch(data.result.data, index);
          }
        }
        advsearchController.set('model', data.result);
        if (data.result.data.length > 0) {
          advsearchController.set('showNoResult', false);
        } else {
          advsearchController.set('showNoResult', true);
        }
      }
      advsearchController.set('loading', false)
    });
  }
});
