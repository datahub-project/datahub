import Route from '@ember/routing/route';

export default Route.extend({
  // maintains backwards compatibility with legacy code
  // TODO: [DSS-6122] refactor so this may not be required
  controllerName: 'flows',

  setupController: function(controller, model, transition) {
    currentTab = 'Flows';
    updateActiveTab();
    this.controller.set('flowView', false);
    if (
      transition &&
      transition.resolvedModels &&
      transition.resolvedModels.flowsname &&
      transition.resolvedModels.flowsname.name &&
      transition.resolvedModels.flow &&
      transition.resolvedModels.flow.id &&
      transition.resolvedModels.pagedflow &&
      transition.resolvedModels.pagedflow.page
    ) {
      var application = transition.resolvedModels.flowsname.name;
      var project = transition.resolvedModels.pagedflow.urn;
      var flow = transition.resolvedModels.flow.id;
      var lineageUrl = '/lineage/flow/' + application + '/' + project + '/' + flow;
      controller.set('lineageUrl', lineageUrl);
      var listUrl = 'api/v1/list/flows/' + application + '/' + project;
      $.get(listUrl, function(data) {
        if (data && data.status == 'ok') {
          // renderFlowListView(data.nodes, flow);
        }
      });
      var url = 'api/v1/flow/' + application + '/' + flow + '?size=10&page=' + transition.resolvedModels.pagedflow.page;
      $.get(url, function(data) {
        if (data && data.status == 'ok') {
          controller.set('model', data);
          controller.set('flowId', flow);
          var breadcrumbs = [
            { title: 'FLOWS_ROOT', urn: 'page/1' },
            { title: application, urn: 'name/' + application + '/page/1?urn=' + application },
            { title: project, urn: 'name/' + project + '/page/1?urn=' + application + '/' + project },
            { title: data.data.flow, urn: 'name/' + application + '/' + flow + '/page/1?urn=' + project }
          ];
          controller.set('breadcrumbs', breadcrumbs);
          if (data.data.flow) {
            // findAndActiveFlowNode(application, project, flow, data.data.flow);
          }
        }
      });
      var watcherEndpoint = '/api/v1/urn/watch?urn=' + application + '/' + project + '/' + flow;
      $.get(watcherEndpoint, function(data) {
        if (data.id && data.id !== 0) {
          controller.set('urnWatched', true);
          controller.set('urnWatchedId', data.id);
        } else {
          controller.set('urnWatched', false);
          controller.set('urnWatchedId', 0);
        }
      });
    }
  }
});
