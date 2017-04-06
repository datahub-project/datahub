import Ember from 'ember';

export default Ember.Route.extend({
  setupController: function (controller, model) {
    const metricsController = this.controllerFor('metrics');

    if (metricsController) {
      metricsController.set('detailview', true);
    }
    currentTab = 'Metrics';
    updateActiveTab();
    var name;
    var id = 0;
    if (model && model.id) {
      var url = 'api/v1/metrics/' + model.id;
      id = model.id;
      if (model.category) {
        name = '{' + model.category + '} ' + model.name;
      }
      else {
        name = model.name;
      }
      var breadcrumbs;
      $.get(url, function (data) {
        if (data && data.status == "ok") {
          controller.set("model", data.metric);
          var dashboard = data.metric.dashboardName;
          if (!dashboard) {
            dashboard = '(Other)';
          }
          var group = data.metric.group;
          if (!group) {
            group = '(Other)';
          }
          breadcrumbs = [{"title": "METRICS_ROOT", "urn": "page/1"},
            {"title": dashboard, "urn": "name/" + dashboard + "/page/1"},
            {"title": group, "urn": "name/" + dashboard + "/" + group + "/page/1"},
            {"title": data.metric.name, "urn": model.id}];
          controller.set('breadcrumbs', breadcrumbs);
          setTimeout(initializeXEditable(id,
              data.metric.description,
              data.metric.dashboardName,
              data.metric.sourceType,
              data.metric.grain,
              data.metric.displayFactor,
              data.metric.displayFactorSym), 500);
        }
      });
    }
    else if (model && model.metric) {
      id = model.metric.id;
      var dashboard = model.metric.dashboardName;
      if (!dashboard) {
        dashboard = '(Other)';
      }
      var group = model.metric.group;
      if (!group) {
        group = '(Other)';
      }
      breadcrumbs = [{"title": "METRICS_ROOT", "urn": "page/1"},
        {"title": name, "urn": "name/" + dashboard + "/page/1"},
        {"title": group, "urn": "name/" + dashboard + "/" + group + "/page/1"},
        {"title": model.metric.name, "urn": model.id}];
      controller.set('breadcrumbs', breadcrumbs);
      if (model.metric.category) {
        name = '{' + model.metric.category + '} ' + model.metric.name;
      }
      else {
        name = model.metric.name;
      }
      setTimeout(initializeXEditable(id,
          model.metric.description,
          model.metric.dashboardName,
          model.metric.sourceType,
          model.metric.grain,
          model.metric.displayFactor,
          model.metric.displayFactorSym), 500);
    }


    var listUrl = 'api/v1/list/metric/' + id;
    $.get(listUrl, function (data) {
      if (data && data.status == "ok") {
        // renderMetricListView(data.nodes, id);
      }
    });

    if (name) {
      // findAndActiveMetricNode(name, id);
    }

  },
  actions: {
    getMetrics: function () {
      var id = this.get('controller.model.id');
      var listUrl = 'api/v1/list/metrics/' + id;

      $.get(listUrl, function (data) {
        if (data && data.status == "ok") {
          // renderMetricListView(data.nodes, id);
        }
      });

      var url = 'api/v1/metrics/' + this.get('controller.model.id')
      var _this = this
      currentTab = 'Metrics';
      updateActiveTab();
      $.get(url, function (data) {
        if (data && data.status == "ok") {
          _this.set('controller.model', data.metric)
          _this.set('controller.detailview', true);
        }
      });
    }
  }
});
