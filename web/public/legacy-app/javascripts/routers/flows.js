var flowsController = null;
App.FlowsRoute = Ember.Route.extend({
    setupController: function(controller) {
        flowsController = controller;
    }
});

App.FlowspageRoute = Ember.Route.extend({
    setupController: function(controller, params) {
        currentTab = 'Flows';
        flowsController.set('flowView', true);
        flowsController.set('queryParams', null);
        var listUrl = 'api/v1/list/flows';
        $.get(listUrl, function(data) {
            if (data && data.status == "ok"){
                renderFlowListView(data.nodes);
            }
        });
        updateActiveTab();
        var url = 'api/v1/flows?size=10&page=' + params.page;
        var breadcrumbs = [{"title": 'FLOWS_ROOT', "urn": "page/1"}];
        $.get(url, function(data) {
            if (data && data.status == "ok"){
                flowsController.set('model', data);
                flowsController.set('jobView', false);
                flowsController.set('flowView', true);
                flowsController.set('breadcrumbs', breadcrumbs);
            }
        });
    }
});

App.FlowsnameRoute = Ember.Route.extend({
    setupController: function(controller, param) {
        flowsController.set('currentName', param.name);
    }
});

App.FlowssubpageRoute = Ember.Route.extend({
    setupController: function (controller, params) {
        if (!flowsController)
            return;

        currentTab = 'Flows';
        updateActiveTab();
        flowsController.set('flowView', true);
        var application;
        var project;
        var urn = params.urn;
        flowsController.set('queryParams', params.urn);
        if (!urn)
         return;

        var links = urn.split('/');
        if (links.length == 1)
        {
            application = links[0];
            flowsController.set('currentName', application);
            var breadcrumbs = [{"title": 'FLOWS_ROOT', "urn": "page/1"},
                {"title": application, "urn": "name/" + application + "/page/1?urn=" + application}];
            var listUrl = 'api/v1/list/flows/' + application;
            $.get(listUrl, function(data) {
                if (data && data.status == "ok"){
                    renderFlowListView(data.nodes);
                }
            });
            var url = 'api/v1/flows/' + application + '?size=10&page=' + params.page;
            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    flowsController.set('model', data);
                    flowsController.set('breadcrumbs', breadcrumbs);
                }
            });
            if (application)
            {
                findAndActiveFlowNode(application, null, null, null);
            }
        }
        else if (links.length == 2)
        {
            application = links[0];
            project = links[1];
            flowsController.set('currentName', project);
            var url = 'api/v1/flows/' + application + '/' + project + '?size=10&page=' + params.page;
            var listUrl = 'api/v1/list/flows/' + application + '/' + project;
            $.get(listUrl, function(data) {
                if (data && data.status == "ok"){
                    renderFlowListView(data.nodes);
                }
            });
            var breadcrumbs = [{"title": 'FLOWS_ROOT', "urn": "page/1"},
                {"title": application, "urn": "name/" + application + "/page/1?urn=" + application},
                {"title": project, "urn": "name/" + project + "/page/1?urn=" + application + '/' + project}];
            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    flowsController.set('model', data);
                    flowsController.set('breadcrumbs', breadcrumbs);
                    flowsController.set('flowView', true);
                    flowsController.set('jobView', false);
                    findAndActiveFlowNode(application, project, null, null);
                }
            });
        }

    }
});

App.PagedflowRoute = Ember.Route.extend({
    setupController: function(controller, params, transition) {
        currentTab = 'Flows';
        updateActiveTab();
        flowsController.set('flowView', false);
        if (transition
            && transition.resolvedModels
            && transition.resolvedModels.flowsname
            && transition.resolvedModels.flowsname.name
            && transition.resolvedModels.flow
            && transition.resolvedModels.flow.id
            && transition.resolvedModels.pagedflow
            && transition.resolvedModels.pagedflow.page )
        {
            var application = transition.resolvedModels.flowsname.name;
            var project = transition.resolvedModels.pagedflow.urn;
            var flow = transition.resolvedModels.flow.id;
            var lineageUrl = '/lineage/flow/' + application + '/' + project + '/' + flow;
            controller.set('lineageUrl', lineageUrl);
            var listUrl = 'api/v1/list/flows/' + application + '/' + project;
            $.get(listUrl, function(data) {
                if (data && data.status == "ok"){
                    renderFlowListView(data.nodes, flow);
                }
            });
            var url = 'api/v1/flow/' + application + '/' + flow + '?size=10&page=' +
                transition.resolvedModels.pagedflow.page;
            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    controller.set('model', data);
                    controller.set('flowId', flow);
                    var breadcrumbs = [{"title": 'FLOWS_ROOT', "urn": "page/1"},
                        {"title": application, "urn": "name/" + application + "/page/1?urn=" + application},
                        {"title": project, "urn": "name/" + project + "/page/1?urn=" + application + '/' + project},
                        {"title": data.data.flow, "urn": "name/" + application + "/" + flow + "/page/1?urn=" + project}];
                    controller.set('breadcrumbs', breadcrumbs);
                    if (data.data.flow)
                    {
                        findAndActiveFlowNode(application, project, flow, data.data.flow);
                    }
                }
            });
            var watcherEndpoint = "/api/v1/urn/watch?urn=" + application + "/" + project + "/" + flow;
            $.get(watcherEndpoint, function(data){
                if(data.id && data.id !== 0) {
                    controller.set('urnWatched', true)
                    controller.set('urnWatchedId', data.id)
                } else {
                    controller.set('urnWatched', false)
                    controller.set('urnWatchedId', 0)
                }
            });
        }
    }
});
