import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

export default Route.extend(AuthenticatedRouteMixin, {
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

      $.get(url, data => {
        if (data && data.status == 'ok') {
          this.controller.set('model', data);
          this.controller.set('urn', null);
          this.controller.set('detailview', false);
        }
      });
    }
  }
});
