import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';


const listURL = '/api/v1/list/datasets`';
const datasetsPageBaseURL = '/api/v1/datasets?size=10&page=';

export default Ember.Route.extend(AuthenticatedRouteMixin, {
  actions: {
    getDatasets: function () {
      const datasetsPageURL = `${datasetsPageBaseURL}${this.controller.get('model.data.page')}`;

      Ember.$.get(listURL, function (data) {
        if (data && data.status == "ok") {
          // renderDatasetListView(data.nodes);
        }
      });

      currentTab = 'Datasets';
      updateActiveTab();
      Ember.$.get(datasetsPageURL, data => {
        if (data && data.status == "ok") {
          this.controller.set('model', data);
          this.controller.set('urn', null);
          this.controller.set('detailview', false);
        }
      });
    }
  }
});