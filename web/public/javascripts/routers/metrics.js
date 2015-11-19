var metricsController = null;
App.MetricsRoute = Ember.Route.extend({
    setupController: function(controller) {
        metricsController = controller;
    },
    actions: {
      getMetrics: function() {
        var url = 'api/v1/metrics?size=10&page=' + metricsController.get('model.data.page');
        currentTab = 'Metric';
        updateActiveTab();
        $.get(url, function(data) {
          if (data && data.status == "ok"){
            metricsController.set('model', data);
            metricsController.set('urn', null);
            metricsController.set('detailview', false);
          }
        });
      }
    }
});
App.MetricspageRoute = Ember.Route.extend({
    setupController: function(controller, param) {
        var url = 'api/v1/metrics?size=10&page=' + param.page;
        currentTab = 'Metric';
        updateActiveTab();
        var breadcrumbs = [{"title":"METRICS_ROOT", "urn":"page/1"}];
        $.get(url, function(data) {
            if (data && data.status == "ok"){
                metricsController.set('model', data);
                metricsController.set('detailview', false);
                metricsController.set('breadcrumbs', breadcrumbs);
                metricsController.set('urn', null);
                metricsController.set('dashboard', null);
                metricsController.set('group', null);
            }
        });
        var watcherEndpoint = "/api/v1/urn/watch?urn=METRICS_ROOT";
        $.get(watcherEndpoint, function(data){
            if(data.id && data.id !== 0) {
                metricsController.set('urnWatched', true)
                metricsController.set('urnWatchedId', data.id)
            } else {
                metricsController.set('urnWatched', false)
                metricsController.set('urnWatchedId', 0)
            }
        })
    },
    actions: {
      getMetrics: function() {
        var url = 'api/v1/metrics?size=10&page=' + metricsController.get('model.data.page');
        currentTab = 'Metric';
        updateActiveTab();
        $.get(url, function(data) {
          if (data && data.status == "ok"){
            metricsController.set('model', data);
            metricsController.set('urn', null);
            metricsController.set('detailview', false);
          }
        });
      }
    }
});

App.MetricRoute = Ember.Route.extend({
    setupController: function(controller, params) {
        if(!metricsController)
            return;
        if (metricsController)
        {
            metricsController.set('detailview', true);
        }
        var name;
        var id = 0;
        if (params && params.id) {
            var url = 'api/v1/metrics/' + params.id;
            id = params.id;
            if (params.category)
            {
                name =  '{' + params.category + '}' + params.name;
            }
            else
            {
                name = params.name;
            }
            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    controller.set("model", data.metric);
                }
            });
        }
        else if (params && params.metric)
        {
            id = params.metric.id;
            if (params.metric.category)
            {
                name =  '{' + params.metric.category + '}' + params.metric.name;
            }
            else
            {
                name =  params.metric.name;
            }
            controller.set("model", params.metric);
        }
        if (name)
        {
            findAndActiveMetricNode(name, id);
        }

    },
    model: function(params) {
        currentTab = 'Metric';
        updateActiveTab()
        if (metricsController)
        {
            metricsController.set('detailview', true);
        }
        return Ember.$.getJSON('api/v1/metrics/' + params.id);
    },
    actions: {
      getMetrics: function() {
        var url = 'api/v1/metrics/' + this.get('controller.model.id')
        var _this = this
        currentTab = 'Metric';
        updateActiveTab();
        $.get(url, function(data) {
          if (data && data.status == "ok"){
            _this.set('controller.model', data.metric)
            _this.set('controller.detailview', true);
          }
        });
      }
    }
});

App.MetricnamepageRoute = Ember.Route.extend({
    model: function(params, transition) {
        currentTab = 'Metric';
        updateActiveTab();
        if (transition
            && transition.resolvedModels
            && transition.resolvedModels.metricname
            && transition.resolvedModels.metricname.name)
        {
            var name = transition.resolvedModels.metricname.name;
            var url = 'api/v1/metrics/name/' + name + '?page=' + params.page;
            var breadcrumbs = [{"title":name, "urn":"name/" + name + "/page/1"}];
            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    metricsController.set('model', data);
                    metricsController.set('detailview', false);
                    metricsController.set('breadcrumbs', breadcrumbs);
                    metricsController.set('urn', name);
                    metricsController.set('dashboard', name);
                    metricsController.set('group', null);
                }
            });
            var watcherEndpoint = "/api/v1/urn/watch?urn=" + name;
            $.get(watcherEndpoint, function(data){
                if(data.id && data.id !== 0) {
                    metricsController.set('urnWatched', true)
                    metricsController.set('urnWatchedId', data.id)
                } else {
                    metricsController.set('urnWatched', false)
                    metricsController.set('urnWatchedId', 0)
                }
            });

            if (name)
            {
                findAndActiveMetricDashboardNode(name);
            }
        }
    },
    actions: {
      getMetrics: function() {
        var url = 'api/v1/metrics/name/' + metricsController.get('dashboard')
        url += '?size=10&page=' + metricsController.get('model.data.page');
        currentTab = 'Metric';
        updateActiveTab();
        $.get(url, function(data) {
          if (data && data.status == "ok"){
            metricsController.set('model', data);
            metricsController.set('urn', null);
            metricsController.set('detailview', false);
          }
        });
      }
    }
});


App.MetricnamesubpageRoute = Ember.Route.extend({
    model: function(params, transition) {
        currentTab = 'Metric';
        updateActiveTab();
        if (transition
            && transition.resolvedModels
            && transition.resolvedModels.metricname
            && transition.resolvedModels.metricname.name)
        {
            var name = transition.resolvedModels.metricname.name;
            var group = '';
            var breadcrumbs;
            var url = 'api/v1/metrics/name/' + name;
            if (transition.resolvedModels.metricgroup && transition.resolvedModels.metricgroup.group)
            {
                group = transition.resolvedModels.metricgroup.group;
                url += '/' + group + '?page=' + params.page;
                breadcrumbs = [{"title":name, "urn":"name/" + name + "/page/1"},
                    {"title":group, "urn":"name/" + name + "/" + group + "/page/1"}];

            }
            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    metricsController.set('breadcrumbs', breadcrumbs);
                    metricsController.set('model', data);
                    metricsController.set('detailview', false);
                    metricsController.set('urn', name + "/" + group);
                    metricsController.set('dashboard', name);
                    metricsController.set('group', group);
                }
            });
            var watcherEndpoint = "/api/v1/urn/watch?urn=" + name + "/" + group;
            $.get(watcherEndpoint, function(data){
                if(data.id && data.id !== 0) {
                    metricsController.set('urnWatched', true)
                    metricsController.set('urnWatchedId', data.id)
                } else {
                    metricsController.set('urnWatched', false)
                    metricsController.set('urnWatchedId', 0)
                }
            });
            if (name && group)
            {
                findAndActiveMetricGroupNode(name, group);
            }
        }
    },
    actions: {
      getMetrics: function() {
        var url = 'api/v1/metrics/name/' + metricsController.get('dashboard')
        url += '/' + metricsController.get('group') + '?size=10&page=' + metricsController.get('model.data.page');
        currentTab = 'Metric';
        updateActiveTab();
        $.get(url, function(data) {
          if (data && data.status == "ok"){
            metricsController.set('model', data);
            metricsController.set('urn', null);
            metricsController.set('detailview', false);
          }
        });
      }
    }
});
