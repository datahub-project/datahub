var App = Ember.Application.create({rootElement: "#contentSplitter"});

if (Ember.Debug && typeof Ember.Debug.registerDeprecationHandler === 'function') {
    Ember.Debug.registerDeprecationHandler(function(message, options, next) {
        if (options && options.id && options.id == 'ember-routing.router-resource') {
          return;
        }
        next(message, options);
    });
}

App.Router.map(function() {
    this.resource('datasets', function(){
        this.resource('page', {path: '/page/:page'});
        this.resource('dataset', {path: '/:id'});
        this.resource('name', {path: '/name/:name'}, function(){
            this.resource('subpage', {path: '/page/:page'});
        });
    });

    this.resource('search');
    this.resource('advsearch');

    this.resource('metrics', function(){
        this.resource('metricspage', {path: '/page/:page'});
        this.resource('metric', {path: '/:id'});
        this.resource('metricname', {path: '/name/:name'}, function(){
            this.resource('metricnamepage', {path: '/page/:page'});
            this.resource('metricgroup', {path: '/:group'}, function(){
                this.resource('metricnamesubpage', {path: '/page/:page'});
            });
        });
    });

    this.resource('flows', function(){
        this.resource('flowspage', {path: '/page/:page'});
        this.resource('flowsname', {path: '/name/:name'}, function(){
            this.resource('flowssubpage', {path: '/page/:page'});
            this.resource('flow', {path: '/:id'}, function(){
                this.resource('pagedflow', {path: '/page/:page'});
            });
        });
    });
});

App.Router.reopen({
    rootURL: '/datasets/page/1',
    tracking: function(){
        var states = this.router.state.handlerInfos;
        var type = "dataset";
        var objectId = 0;
        var objectName = "";
        var params = '';
        if (states)
        {
            var currentState = states[states.length - 1];
            if (currentState && currentState.name)
            {
                switch(currentState.name)
                {
                    case "page":
                        type = "dataset";
                        params = JSON.stringify(currentState.params);
                        break;
                    case "subpage":
                        type = "dataset";
                        if (currentState.context)
                        {
                            params = JSON.stringify(currentState.context);
                        }
                        else
                        {
                            params = JSON.stringify(currentState.params);
                        }
                        break;
                    case "dataset":
                        type = "dataset";
                        params = JSON.stringify(currentState.params);
                        if (currentState.context.dataset)
                        {
                            objectId =  currentState.context.dataset.id;
                            objectName = currentState.context.dataset.name;
                        }
                        else
                        {
                            objectId =  currentState.context.id;
                            objectName = currentState.context.name;
                        }
                        break;
                    case "metricspage":
                        type = "metric";
                        if (currentState.context)
                        {
                            params = JSON.stringify(currentState.context);
                        }
                        else
                        {
                            params = JSON.stringify(currentState.params);
                        }
                        break;
                    case "metricnamepage":
                        type = "metric";
                        var previousState = states[states.length - 2];
                        if (previousState && previousState.name == 'metricname')
                        {
                            params = JSON.stringify(
                                {'name': previousState.context.name, 'page': currentState.params.page});
                        }
                        else
                        {
                            params = JSON.stringify(currentState.params);
                        }
                        break;
                    case "metricnamesubpage":
                        type = "metric";
                        var previousState = states[states.length - 2];
                        var parentState = states[states.length - 3];
                        if (previousState && previousState.name == 'metricgroup'
                            && parentState && parentState.name == 'metricname')
                        {
                            params = JSON.stringify(
                                {'name': parentState.context.name,
                                 'group': previousState.context.group,
                                 'page': currentState.params.page});
                        }
                        else
                        {
                            params = JSON.stringify(currentState.params);
                        }
                        break;
                    case "metric":
                        type = "metric";
                        params = JSON.stringify(currentState.params);
                        if (currentState.context.metric)
                        {
                            objectId =  currentState.context.metric.id;
                            objectName = currentState.context.metric.name;
                        }
                        else
                        {
                            objectId =  currentState.context.id;
                            objectName = currentState.context.name;
                        }
                        break;
                    case "pagedapplication":
                        type = "flow";
                        var applicationState = states[states.length - 2];
                        if (applicationState && applicationState.name == 'applicationname')
                        {
                            params = JSON.stringify(
                                {'name': applicationState.context.applicationname, 'page': currentState.params.page});
                        }
                        else
                        {
                            params = JSON.stringify(currentState.params);
                        }
                        break;
                    case "pagedproject":
                        type = "flow";
                        var projectState = states[states.length - 2];
                        var applicationState = states[states.length - 3];
                        if (projectState && projectState.name == 'project'
                            && applicationState && applicationState.name == 'applicationname')
                        {
                            params = JSON.stringify(
                                {'application': applicationState.context.applicationname,
                                 'project': projectState.context.project,
                                 'page': currentState.params.page});
                        }
                        else
                        {
                            params = JSON.stringify(currentState.params);
                        }
                        break;
                    case "pagedflow":
                        type = "flow";
                        var flowState = states[states.length - 2];
                        var projectState = states[states.length - 3];
                        var applicationState = states[states.length - 4];
                        if (flowState && flowState.name == 'flow' && projectState && projectState.name == 'project'
                            && applicationState && applicationState.name == 'applicationname')
                        {
                            params = JSON.stringify(
                                {'application': applicationState.context.applicationname,
                                 'project': projectState.context.project,
                                 'flow': flowState.context.flow,
                                 'page': currentState.params.page});
                        }
                        else
                        {
                            params = JSON.stringify(currentState.params);
                        }
                        break;
                    default:
                }
            }

        }
        var trackingObject = {
            object_type: type,
            object_id: objectId,
            object_name: objectName,
            parameters: params
        };
        //window.InternalTracking.track(trackingObject);
        return true;
    }.on('didTransition')
});

App.IndexRoute = Ember.Route.extend({
    redirect: function() {
        this.transitionTo('page', 1);
    }
});

App.ApplicationController = Ember.Controller.extend({
    source: ['hdfs', 'teradata']
});

function updateActiveTab()
{
    var obj = $("#mainnavbar .nav").find(".active");
    if (obj)
    {
        obj.removeClass("active");
    }

    if (currentTab == 'Metrics')
    {
        $('#menutabs a:eq(1)').tab("show");
        $('#metriclink').addClass("active");

    }
    else if (currentTab == 'Flows')
    {
        $('#menutabs a:last').tab("show");
        $('#flowlink').addClass("active");
    }
    else
    {
        $('#menutabs a:first').tab("show");
        $('#datasetlink').addClass("active");
    }
}

function findAndActiveDatasetNode(name, urn){
    var rootNode = $("#tree2").fancytree("getRootNode");
    if (!rootNode.isLoading())
    {
        var nodes = rootNode.findAll(name);
        for(var i in nodes){
            if (nodes[i] && nodes[i].data && nodes[i].data.path == urn){
                window.g_skipDatasetTreeActivation = true;
                nodes[i].setActive();
                scrollToTreeNode();
                return;
            }
        }
    }
    else
    {
        window.g_currentDatasetNodeName = name;
        window.g_currentDatasetNodeUrn = urn;
    }
};

function findAndActiveMetricNode(name, id){
    var rootNode = $("#tree1").fancytree("getRootNode");
    if (!rootNode.isLoading())
    {
        var nodes = rootNode.findAll(name);
        for(var i in nodes){
            if (nodes[i] && nodes[i].data && nodes[i].data.metric_id == id){
                window.g_skipMetricTreeActivation = true;
                nodes[i].setActive();
                scrollToTreeNode();
                return;
            }
        }
    }
    else
    {
        window.g_currentMetricNodeName = name;
        window.g_currentMetricNodeId = id;
        window.g_currentMetricDashboardName = null;
        window.g_currentMetricGroupName = null;
    }
};

function findAndActiveMetricDashboardNode(name){
    var rootNode = $("#tree1").fancytree("getRootNode");
    if (!rootNode.isLoading())
    {
        var nodes = rootNode.findAll(name);
        for(var i in nodes){
            if (nodes[i] && nodes[i].data && nodes[i].data.dashboard_name == name && nodes[i].data.level == 1){
                window.g_skipMetricTreeActivation = true;
                nodes[i].setActive();
                scrollToTreeNode();
                return;
            }
        }
    }
    else
    {
        window.g_currentMetricDashboardName = name;
        window.g_currentMetricGroupName = null;
        window.g_currentMetricNodeName = null;
        window.g_currentMetricNodeId = null;
    }
};

function findAndActiveMetricGroupNode(dashboard, group){
    var rootNode = $("#tree1").fancytree("getRootNode");
    if (!rootNode.isLoading())
    {
        var nodes = rootNode.findAll(group);
        for(var i in nodes){
            if (nodes[i] && nodes[i].data && nodes[i].data.dashboard_name == dashboard
                && nodes[i].data.metric_group == group
                && nodes[i].data.level == 2){
                window.g_skipMetricTreeActivation = true;
                nodes[i].setActive();
                scrollToTreeNode();
                return;
            }
        }
    }
    else
    {
        window.g_currentMetricDashboardName = dashboard;
        window.g_currentMetricGroupName = group;
        window.g_currentMetricNodeName = null;
        window.g_currentMetricNodeId = null;
    }
};

function findAndActiveFlowNode(application, project, flowId, flowName){
    var rootNode = $("#tree3").fancytree("getRootNode");
    if (!rootNode.isLoading())
    {
        var node = $("#tree3").fancytree("getActiveNode");
        var name;
        var level = 0;
        if (flowName)
        {
            name = flowName;
            level = 2;
        }
        else if (project)
        {
            name = project;
            level = 1;
        }
        else if (application)
        {
            name = application;
            level = 0;
        }
        else
        {
            return;
        }

        if (node && node.title == name)
        {
            return;
        }

        var nodes = rootNode.findAll(name);
        if (nodes && nodes.length > 0)
        {
            for(var i = 0; i< nodes.length; i++)
            {
                if (level == 0)
                {
                    if (nodes[i].title == application)
                    {
                        window.g_skipFlowTreeActivation = true;
                        nodes[i].setActive();
                        scrollToTreeNode();
                        return;
                    }
                }
                else if (level == 1)
                {
                    if (nodes[i] && nodes[i].parent)
                    {
                        if (nodes[i].title == project && nodes[i].parent.title == application)
                        {
                            window.g_skipFlowTreeActivation = true;
                            nodes[i].setActive();
                            scrollToTreeNode();
                            return;
                        }
                    }
                }
                else if (level == 2)
                {
                    if (nodes[i] && nodes[i].parent && nodes[i].parent.parent)
                    {
                        var topParent = nodes[i].parent.parent;
                        if (nodes[i].data.id == flowId
                            && nodes[i].parent.title == project && topParent.title == application)
                        {
                            window.g_skipFlowTreeActivation = true;
                            nodes[i].setActive();
                            scrollToTreeNode();
                            return;
                        }
                    }
                }
            }
        }
    }
    else
    {
        window.g_currentFlowApplication = application;
        window.g_currentFlowProject = project;
        window.g_currentFlowId = flowId;
        window.g_currentFlowName = flowName;
    }
};

var scrollToTreeNode = function() {
    $("#tabSplitter").scrollTo($('.fancytree-focused'), 800)
}

$('.category-header a').click(function (e) {
    var text = 'Datasets';
    if (this && this.children && this.children.length > 0 && this.children[0] && this.children[0].textContent)
    {
        text = this.children[0].textContent;
    }
    if (currentTab == text)
        return;
    currentTab = text;
    updateActiveTab();
    if (text == 'Datasets')
    {
        var node = $("#tree2").fancytree("getActiveNode");
        if (node)
        {
            node.setActive(false);
            node.setFocus(false);
        }
        window.location = "#/datasets/page/1";
    }
    else if (text == 'Metrics')
    {
        var node = $("#tree1").fancytree("getActiveNode");
        if (node)
        {
            node.setActive(false);
            node.setFocus(false);
        }
        window.location = "#/metrics/page/1";
    }
    else if (text == 'Flows')
    {
        var node = $("#tree3").fancytree("getActiveNode");
        if (node)
        {
            node.setActive(false);
            node.setFocus(false);
        }
        window.location = "#/flows/page/1";
    }
});
