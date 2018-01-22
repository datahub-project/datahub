import Route from '@ember/routing/route';

export default Route.extend({
  model: function(params, transition) {
    currentTab = 'Metrics';
    updateActiveTab();
    if (
      transition &&
      transition.resolvedModels &&
      transition.resolvedModels.metricname &&
      transition.resolvedModels.metricname.name
    ) {
      var name = transition.resolvedModels.metricname.name;
      var listUrl = 'api/v1/list/metrics/' + name;
      $.get(listUrl, function(data) {
        if (data && data.status == 'ok') {
          // renderMetricListView(data.nodes);
        }
      });

      var url = 'api/v1/metrics/name/' + name + '?page=' + params.page;
      var breadcrumbs = [{ title: 'METRICS_ROOT', urn: 'page/1' }, { title: name, urn: 'name/' + name + '/page/1' }];
      $.get(url, data => {
        if (data && data.status == 'ok') {
          this.controllerFor('metrics').set('model', data);
          this.controllerFor('metrics').set('detailview', false);
          this.controllerFor('metrics').set('breadcrumbs', breadcrumbs);
          this.controllerFor('metrics').set('urn', name);
          this.controllerFor('metrics').set('dashboard', name);
          this.controllerFor('metrics').set('group', null);
        }
      });
      var watcherEndpoint = '/api/v1/urn/watch?urn=' + name;
      $.get(watcherEndpoint, data => {
        if (data.id && data.id !== 0) {
          this.controllerFor('metrics').set('urnWatched', true);
          this.controllerFor('metrics').set('urnWatchedId', data.id);
        } else {
          this.controllerFor('metrics').set('urnWatched', false);
          this.controllerFor('metrics').set('urnWatchedId', 0);
        }
      });
    }
  },
  actions: {
    getMetrics: function() {
      var listUrl = 'api/v1/list/metrics/' + this.controllerFor('metrics').get('dashboard');
      $.get(listUrl, function(data) {
        if (data && data.status == 'ok') {
          // renderMetricListView(data.nodes);
        }
      });

      var url = 'api/v1/metrics/name/' + this.controllerFor('metrics').get('dashboard');
      url += '?size=10&page=' + this.controllerFor('metrics').get('model.data.page');
      currentTab = 'Metrics';
      updateActiveTab();
      $.get(url, data => {
        if (data && data.status == 'ok') {
          this.controllerFor('metrics').set('model', data);
          this.controllerFor('metrics').set('urn', null);
          this.controllerFor('metrics').set('detailview', false);
        }
      });
    }
  }
});
