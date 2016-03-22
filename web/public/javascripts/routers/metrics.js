var metricsController = null;
App.MetricsRoute = Ember.Route.extend({
    setupController: function(controller) {
        metricsController = controller;
    },
    actions: {
      getMetrics: function() {
        var url = 'api/v1/metrics?size=10&page=' + metricsController.get('model.data.page');
        currentTab = 'Metrics';
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
        currentTab = 'Metrics';
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
        currentTab = 'Metrics';
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

var update = function(param)
{
    if (param && param.name)
    {
        var name = param.name;
        var val = param.value;
        var metricId = param.pk;
        var url = '/api/v1/metrics/' + metricId + '/update';
        var method = 'POST';
        var token = $("#csrfToken").val().replace('/', '');
        var data = {"csrfToken": token};
        data[name] = val;
        $.ajax({
            url: url,
            method: method,
            headers: {
                'Csrf-Token': token
            },
            dataType: 'json',
            data: data
        }).done(function(data, txt, xhr){
            if(data && data.status && data.status == "success")
            {
                console.log('Done.')
            }
            else
            {
                console.log('Failed.')
            }
        }).fail(function(xhr, txt, err){
            Notify.toast("Failed to update data", "Metric Update Failure", "error")
        })
    }

}
function initializeXEditable(
    id,
    description,
    dashboardName,
    sourceType,
    grain,
    displayFactor,
    displayFactorSym)
{
    $.fn.editable.defaults.mode = 'inline';

    //below code is a walk around for xeditable and ember integration issue

    $('.xeditable').editable("disable");
    $('.xeditable').editable("destroy");

    $('#metricdesc').text(description);
    $('#metricdesc').editable({
        pk: id,
        value: description,
        url: update
    });

    $('#dashboardname').text(dashboardName);
    $('#dashboardname').editable({
        pk: id,
        value: dashboardName,
        url: update
    });

    $('#sourcetype').text(sourceType);
    $('#sourcetype').editable({
        pk: id,
        value: sourceType,
        url: update
    });

    $('#metricgrain').text(grain);
    $('#metricgrain').editable({
        pk: id,
        value: grain,
        url: update
    });

    $('#displayfactor').text(displayFactor);
    $('#displayfactor').editable({
        pk: id,
        value: displayFactor,
        url: update
    });

    $('#displayfactorsym').text(displayFactorSym);
    $('#displayfactorsym').editable({
        pk: id,
        value: displayFactorSym,
        url: update
    });
}

App.MetricRoute = Ember.Route.extend({
    setupController: function(controller, params) {
        if(!metricsController)
            return;
        if (metricsController)
        {
            metricsController.set('detailview', true);
        }
        currentTab = 'Metrics';
        updateActiveTab();
        var name;
        var id = 0;
        if (params && params.id) {
            var url = 'api/v1/metrics/' + params.id;
            id = params.id;
            if (params.category)
            {
                name =  '{' + params.category + '} ' + params.name;
            }
            else
            {
                name = params.name;
            }
            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    controller.set("model", data.metric);
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
        else if (params && params.metric)
        {
            id = params.metric.id;
            if (params.metric.category)
            {
                name =  '{' + params.metric.category + '} ' + params.metric.name;
            }
            else
            {
                name =  params.metric.name;
            }
            setTimeout(initializeXEditable(id,
                params.metric.description,
                params.metric.dashboardName,
                params.metric.sourceType,
                params.metric.grain,
                params.metric.displayFactor,
                params.metric.displayFactorSym), 500);
        }
        if (name)
        {
            findAndActiveMetricNode(name, id);
        }

    },
    actions: {
      getMetrics: function() {
        var url = 'api/v1/metrics/' + this.get('controller.model.id')
        var _this = this
        currentTab = 'Metrics';
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
        currentTab = 'Metrics';
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
        currentTab = 'Metrics';
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
        currentTab = 'Metrics';
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
        currentTab = 'Metrics';
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
