import Ember from 'ember';

export default Ember.Route.extend({
  controllerName: 'datasets',

  setupController: function (controller, model) {
    controller.set('currentName', model.name);
  },

  actions: {
    getDatasets: function () {
      var listUrl = '/api/v1/list/datasets';

      $.get(listUrl, function (data) {
        if (data && data.status == "ok") {
          // renderDatasetListView(data.nodes);
        }
      });

      var url = '/api/v1/datasets?size=10&page=' + this.controller.get('model.data.page');
      currentTab = 'Datasets';

      updateActiveTab();

      $.get(url, data => {
        if (data && data.status == "ok") {
          this.controller.set('model', data);
          this.controller.set('urn', null);
          this.controller.set('detailview', false);
        }
      });
    }
  }
});
