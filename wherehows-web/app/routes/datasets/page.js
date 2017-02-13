import Ember from 'ember';

const listUrl = '/api/v1/list/datasets';
const datasetsPageBaseURL = '/api/v1/datasets?size=10&page=';


export default Ember.Route.extend({
  // maintains backwards compatibility with legacy code
  // TODO: [DSS-6122] refactor so this may not be required
  controllerName: 'datasets',

  setupController: function (controller, model) {
    const datasetsPageURL = `${datasetsPageBaseURL}${model.page}`;
    currentTab = 'Datasets';

    $.get(listUrl, function (data) {
      if (data && data.status == "ok") {
        // renderDatasetListView(data.nodes);
      }
    });

    var breadcrumbs = [{"title": "DATASETS_ROOT", "urn": "1"}];
    $.get(datasetsPageURL, data => {
      if (data && data.status == "ok") {
        this.controller.set('model', data);
        this.controller.set('breadcrumbs', breadcrumbs);
        this.controller.set('urn', null);
        this.controller.set('detailview', false);
      }
    });

    var watcherEndpoint = "/api/v1/urn/watch?urn=DATASETS_ROOT";
    $.get(watcherEndpoint, data => {
      if (data.id && data.id !== 0) {
        this.controller.set('urnWatched', true);
        this.controller.set('urnWatchedId', data.id)
      } else {
        this.controller.set('urnWatched', false);
        this.controller.set('urnWatchedId', 0)
      }
    });
  },

  actions: {
    didTransition () {
      Ember.run.scheduleOnce('afterRender', this, updateActiveTab);
    },

    getDatasets: function () {
      const datasetsPageURL = `${datasetsPageBaseURL}${this.controller.get('model.data.page')}`;
      currentTab = 'Datasets';

      $.get(listUrl, function (data) {
        if (data && data.status == "ok") {
          // renderDatasetListView(data.nodes);
        }
      });

      updateActiveTab();
      $.get(datasetsPageURL, data => {
        if (data && data.status === "ok") {
          this.controller.set('model', data);
          this.controller.set('urn', null);
          this.controller.set('detailview', false);
        }
      });
    }
  }
});
