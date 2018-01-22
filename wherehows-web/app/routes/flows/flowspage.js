import Route from '@ember/routing/route';
import $ from 'jquery';

const listUrl = '/api/v1/list/flows';
const flowsPageBaseURL = '/api/v1/flows?size=10&page=';

export default Route.extend({
  // maintains backwards compatibility with legacy code
  // TODO: [DSS-6122] refactor so this may not be required
  controllerName: 'flows',

  setupController: function(controller, model) {
    const flowsPageURL = `${flowsPageBaseURL}${model.page}`;
    const breadcrumbs = [{ title: 'FLOWS_ROOT', urn: '1' }];
    currentTab = 'Flows';

    this.controller.set('flowView', true);
    this.controller.set('queryParams', null);

    $.get(listUrl, function(data) {
      if (data && data.status === 'ok') {
        // renderFlowListView(data.nodes);
      }
    });

    updateActiveTab();

    $.get(flowsPageURL, data => {
      if (data && data.status === 'ok') {
        this.controller.set('model', data);
        this.controller.set('jobView', false);
        this.controller.set('flowView', true);
        this.controller.set('breadcrumbs', breadcrumbs);
      }
    });
  }
});
