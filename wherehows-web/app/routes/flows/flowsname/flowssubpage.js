import Route from '@ember/routing/route';

export default Route.extend({
  // maintains backwards compatibility with legacy code
  // TODO: [DSS-6122] refactor so this may not be required
  controllerName: 'flows',

  setupController: function(controller, model) {
    var application;
    var project;
    var urn = model.urn;
    currentTab = 'Flows';
    updateActiveTab();

    this.controller.set('flowView', true);
    this.controller.set('queryParams', model.urn);
    if (!urn) return;

    var links = urn.split('/');
    if (links.length == 1) {
      application = links[0];
      this.controller.set('currentName', application);
      var breadcrumbs = [
        { title: 'FLOWS_ROOT', urn: 'page/1' },
        { title: application, urn: 'name/' + application + '/page/1?urn=' + application }
      ];
      var listUrl = 'api/v1/list/flows/' + application;
      $.get(listUrl, function(data) {
        if (data && data.status == 'ok') {
          // renderFlowListView(data.nodes);
        }
      });
      var url = 'api/v1/flows/' + application + '?size=10&page=' + model.page;
      $.get(url, function(data) {
        if (data && data.status == 'ok') {
          this.controller.set('model', data);
          this.controller.set('breadcrumbs', breadcrumbs);
        }
      });
      if (application) {
        // findAndActiveFlowNode(application, null, null, null);
      }
    } else if (links.length == 2) {
      application = links[0];
      project = links[1];
      this.controller.set('currentName', project);
      var url = 'api/v1/flows/' + application + '/' + project + '?size=10&page=' + model.page;
      var listUrl = 'api/v1/list/flows/' + application + '/' + project;
      $.get(listUrl, function(data) {
        if (data && data.status == 'ok') {
          // renderFlowListView(data.nodes);
        }
      });
      var breadcrumbs = [
        { title: 'FLOWS_ROOT', urn: 'page/1' },
        { title: application, urn: 'name/' + application + '/page/1?urn=' + application },
        { title: project, urn: 'name/' + project + '/page/1?urn=' + application + '/' + project }
      ];
      $.get(url, function(data) {
        if (data && data.status == 'ok') {
          this.controller.set('model', data);
          this.controller.set('breadcrumbs', breadcrumbs);
          this.controller.set('flowView', true);
          this.controller.set('jobView', false);
          // findAndActiveFlowNode(application, project, null, null);
        }
      });
    }
  }
});
