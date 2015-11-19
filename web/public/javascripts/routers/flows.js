var flowsController = null;
App.FlowsRoute = Ember.Route.extend({
    setupController: function(controller) {
        flowsController = controller;
    }
});

App.PagedapplicationRoute = Ember.Route.extend({
    setupController: function(controller, params, transition) {
        currentTab = 'Flow';
        updateActiveTab();
        if (transition
            && transition.resolvedModels
            && transition.resolvedModels.applicationname
            && transition.resolvedModels.applicationname.applicationname)
        {
            var application = transition.resolvedModels.applicationname.applicationname;
            var breadcrumbs = [{"title": application, "urn": application + "/page/1"}];
            if (application && (application.toLowerCase().indexOf("appworx") != -1))
            {
                flowsController.set('isAppworx', true);
            }
            else
            {
                flowsController.set('isAppworx', false);
            }
            var url = 'api/v1/flows/' + application + '?size=10&page=' + params.page;
            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    flowsController.set('model', data);
                    flowsController.set('projectView', true);
                    flowsController.set('flowView', false);
                    flowsController.set('breadcrumbs', breadcrumbs);
                    flowsController.set('urn', application);
                    flowsController.set('jobView', false);
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
        currentTab = 'Flow';
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
            var breadcrumbs = [{"title": application, "urn": application + "/page/1"},
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
        currentTab = 'Flow';
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
            var url = 'api/v1/flows/' + application + '/' + project + '/' + flow + '?size=10&page=' + params.page;
            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    controller.set('model', data);
                    controller.set('flowId', flow);
                    controller.set('urn', application + '/' + project + '/' + flow);
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
