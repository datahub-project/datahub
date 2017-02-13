import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

export default Ember.Route.extend(AuthenticatedRouteMixin, {
  queryParams: {
    category: {
      refreshModel: true
    },
    keywords: {
      refreshModel: true
    },
    source: {
      refreshModel: true
    },
    page: {
      refreshModel: true
    }
  },
  model: function (params) {
    this.controller.set('loading', true);
    var q;
    if (params) {
      q = params;
    }
    else {
      q = convertQueryStringToObject();
    }

    var keyword = atob(q.keywords);
    var url = 'api/v1/search' + '?page=' + params.page + "&keyword=" + keyword;
    if (q.category) {
      url += ("&category=" + q.category.toLowerCase());
      currentTab = q.category.toProperCase();
      updateActiveTab();
    }
    if (q.source) {
      url += '&source=' + q.source;
    }
    $.get(url, data => {
      if (data && data.status == "ok") {
        var result = data.result;
        var keywords = result.keywords;
        window.g_currentCategory = result.category;
        updateSearchCategories(result.category);
        for (var index = 0; index < result.data.length; index++) {
          var schema = result.data[index].schema;
          if (schema) {
            result.data[index].originalSchema = schema;
            highlightResults(result.data, index, keyword);
          }
        }
        this.controller.set('model', result);
        this.controller.set('keyword', keyword);
        this.controller.set('isMetric', false);
        if (result.data.length > 0) {
          this.controller.set('showNoResult', false);
        } else {
          this.controller.set('showNoResult', true);
        }
        this.controller.set('loading', false)
      }
    });
  }
});