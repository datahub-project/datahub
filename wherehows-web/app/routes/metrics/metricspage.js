import Route from '@ember/routing/route';

export default Route.extend({
  // maintains backwards compatibility with legacy code
  // TODO: [DSS-6122] refactor so this may not be required
  controllerName: 'metrics',

  setupController: function(controller, model) {
    var listUrl = '/api/v1/list/metrics';
    $.get(listUrl, function(data) {
      if (data && data.status == 'ok') {
        // renderMetricListView(data.nodes);
      }
    });
    var url = '/api/v1/metrics?size=10&page=' + model.page;
    currentTab = 'Metrics';
    updateActiveTab();
    var breadcrumbs = [{ title: 'METRICS_ROOT', urn: '1' }];
    $.get(url, data => {
      if (data && data.status == 'ok') {
        this.controller.set('model', data);
        this.controller.set('detailview', false);
        this.controller.set('breadcrumbs', breadcrumbs);
        this.controller.set('urn', null);
        this.controller.set('dashboard', null);
        this.controller.set('group', null);
      }
    });
    var watcherEndpoint = '/api/v1/urn/watch?urn=METRICS_ROOT';
    $.get(watcherEndpoint, data => {
      if (data.id && data.id !== 0) {
        this.controller.set('urnWatched', true);
        this.controller.set('urnWatchedId', data.id);
      } else {
        this.controller.set('urnWatched', false);
        this.controller.set('urnWatchedId', 0);
      }
    });
  },
  actions: {
    getMetrics: function() {
      var listUrl = '/api/v1/list/metrics';
      $.get(listUrl, function(data) {
        if (data && data.status == 'ok') {
          // renderMetricListView(data.nodes);
        }
      });
      var url = '/api/v1/metrics?size=10&page=' + this.controller.get('model.data.page');
      currentTab = 'Metrics';
      updateActiveTab();
      $.get(url, function(data) {
        if (data && data.status == 'ok') {
          this.controller.set('model', data);
          this.controller.set('urn', null);
          this.controller.set('detailview', false);
        }
      });
    }
  }
});
