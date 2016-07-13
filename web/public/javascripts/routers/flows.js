var flowsController = null;
App.FlowsRoute = Ember.Route.extend({
    setupController: function(controller) {
        flowsController = controller;
        var location = window.location.hash;
        if (location != '#/flows/page/1')
        {
            return;
        }
        currentTab = 'Flows';
        var listUrl = 'api/v1/list/flows';
        $.get(listUrl, function(data) {
            if (data && data.status == "ok"){
                renderFlowListView(data.nodes);
            }
        });
        updateActiveTab();
        var url = 'api/v1/flows?size=10&page=1';
        var breadcrumbs = [{"title": 'FLOWS_ROOT', "urn": "page/1"}];
        $.get(url, function(data) {
            if (data && data.status == "ok"){
                flowsController.set('model', data);
                flowsController.set('projectView', true);
                flowsController.set('flowView', false);
                flowsController.set('breadcrumbs', breadcrumbs);
                flowsController.set('urn', 'FLOWS_ROOT');
                flowsController.set('projectView', true);
                flowsController.set('flowView', false);
                flowsController.set('jobView', false);
            }
        });
        var watcherEndpoint = "/api/v1/urn/watch?urn=FLOWS_ROOT";
        $.get(watcherEndpoint, function(data){
            if(data.id && data.id !== 0) {
                flowsController.set('urnWatched', true)
                flowsController.set('urnWatchedId', data.id)
            } else {
                flowsController.set('urnWatched', false)
                flowsController.set('urnWatchedId', 0)
            }
        });
    }
});

App.FlowspageRoute = Ember.Route.extend({
    setupController: function(controller) {
        currentTab = 'Flows';
        var listUrl = 'api/v1/list/flows';
        $.get(listUrl, function(data) {
            if (data && data.status == "ok"){
                renderFlowListView(data.nodes);
            }
        });
        updateActiveTab();
        var url = 'api/v1/flows?size=10&page=1';
        var breadcrumbs = [{"title": 'FLOWS_ROOT', "urn": "page/1"}];
        $.get(url, function(data) {
            if (data && data.status == "ok"){
                flowsController.set('model', data);
                flowsController.set('projectView', true);
                flowsController.set('flowView', false);
                flowsController.set('breadcrumbs', breadcrumbs);
                flowsController.set('urn', 'FLOWS_ROOT');
                flowsController.set('projectView', true);
                flowsController.set('flowView', false);
                flowsController.set('jobView', false);
            }
        });
        var watcherEndpoint = "/api/v1/urn/watch?urn=FLOWS_ROOT";
        $.get(watcherEndpoint, function(data){
            if(data.id && data.id !== 0) {
                flowsController.set('urnWatched', true)
                flowsController.set('urnWatchedId', data.id)
            } else {
                flowsController.set('urnWatched', false)
                flowsController.set('urnWatchedId', 0)
            }
        });
    }
});

App.PagedapplicationRoute = Ember.Route.extend({
    setupController: function(controller, params, transition) {
        currentTab = 'Flows';
        updateActiveTab();
        if (transition
            && transition.resolvedModels
            && transition.resolvedModels.applicationname
            && transition.resolvedModels.applicationname.applicationname)
        {
            var application = transition.resolvedModels.applicationname.applicationname;
            var breadcrumbs = [{"title": 'FLOWS_ROOT', "urn": "page/1"},
                {"title": application, "urn": application + "/page/1"}];
            if (application && (application.toLowerCase().indexOf("appworx") != -1))
            {
                flowsController.set('isAppworx', true);
            }
            else
            {
                flowsController.set('isAppworx', false);
            }
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
                    flowsController.set('projectView', true);
                    flowsController.set('flowView', false);
                    flowsController.set('breadcrumbs', breadcrumbs);
                    flowsController.set('urn', application);
                    flowsController.set('jobView', false);
                    flowsController.set('projectView', true);
                    flowsController.set('flowView', false);
                }
            });
            var watcherEndpoint = "/api/v1/urn/watch?urn=" + application;
            $.get(watcherEndpoint, function(data){
                if(data.id && data.id !== 0) {
                    flowsController.set('urnWatched', true)
                    flowsController.set('urnWatchedId', data.id)
                } else {
                    flowsController.set('urnWatched', false)
                    flowsController.set('urnWatchedId', 0)
                }
            });
            if (application)
            {
                findAndActiveFlowNode(application, null, null, null);
            }
        }
    }
});

App.PagedprojectRoute = Ember.Route.extend({
    setupController: function(controller, params, transition) {
        currentTab = 'Flows';
        updateActiveTab();
        if (transition
            && transition.resolvedModels
            && transition.resolvedModels.applicationname
            && transition.resolvedModels.applicationname.applicationname
            && transition.resolvedModels.project
            && transition.resolvedModels.project.project)
        {
            var application = transition.resolvedModels.applicationname.applicationname;
            if (application && (application.toLowerCase().indexOf("appworx") != -1))
            {
                flowsController.set('isAppworx', true);
            }
            else
            {
                flowsController.set('isAppworx', false);
            }
            var project = transition.resolvedModels.project.project;
            var url = 'api/v1/flows/' + application + '/' + project + '?size=10&page=' + params.page;
            var listUrl = 'api/v1/list/flows/' + application + '/' + project;
            $.get(listUrl, function(data) {
                if (data && data.status == "ok"){
                    renderFlowListView(data.nodes);
                }
            });
            var breadcrumbs = [{"title": 'FLOWS_ROOT', "urn": "page/1"},
                    {"title": application, "urn": application + "/page/1"},
                    {"title": project, "urn": application + "/" + project + "/page/1"}];
            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    flowsController.set('model', data);
                    flowsController.set('breadcrumbs', breadcrumbs);
                    flowsController.set('projectView', false);
                    flowsController.set('flowView', true);
                    flowsController.set('urn', application + "/" + project);
                    flowsController.set('jobView', false);
                    findAndActiveFlowNode(application, project, null, null);
                }
            });
            var watcherEndpoint = "/api/v1/urn/watch?urn=" + application + "/" + project;
            $.get(watcherEndpoint, function(data){
                if(data.id && data.id !== 0) {
                    flowsController.set('urnWatched', true)
                    flowsController.set('urnWatchedId', data.id)
                } else {
                    flowsController.set('urnWatched', false)
                    flowsController.set('urnWatchedId', 0)
                }
            });
        }
    }
});

App.PagedflowRoute = Ember.Route.extend({
    setupController: function(controller, params, transition) {
        currentTab = 'Flows';
        updateActiveTab();
        if (transition
            && transition.resolvedModels
            && transition.resolvedModels.applicationname
            && transition.resolvedModels.applicationname.applicationname
            && transition.resolvedModels.project
            && transition.resolvedModels.project.project
            && transition.resolvedModels.flow
            && transition.resolvedModels.flow.flow)
        {
            flowsController.set('projectView', false);
            flowsController.set('flowView', false);
            var application = transition.resolvedModels.applicationname.applicationname;
            var project = transition.resolvedModels.project.project;
            var flow = transition.resolvedModels.flow.flow;
            var lineageUrl = '/lineage/flow/' + application + '/' + project + '/' + flow;
            controller.set('lineageUrl', lineageUrl);
            var listUrl = 'api/v1/list/flows/' + application + '/' + project;
            console.log(flow);
            $.get(listUrl, function(data) {
                if (data && data.status == "ok"){
                    renderFlowListView(data.nodes, flow);
                }
            });
            var url = 'api/v1/flows/' + application + '/' + project + '/' + flow + '?size=10&page=' + params.page;
            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    controller.set('model', data);
                    controller.set('flowId', flow);
                    controller.set('urn', application + '/' + project + '/' + flow);
                    var breadcrumbs = [{"title": 'FLOWS_ROOT', "urn": "page/1"},
                        {"title": application, "urn": application + "/page/1"},
                        {"title": project, "urn": application + "/" + project + "/page/1"},
                        {"title": data.data.flow, "urn": application + "/" + project + "/" + flow + "/page/1"}];
                    controller.set('breadcrumbs', breadcrumbs);
                    flowsController.set('projectView', false);
                    flowsController.set('flowView', false);
                    flowsController.set('jobView', true);
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
