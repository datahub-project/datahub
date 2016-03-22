(function (window, $) {
    $(document).ready(function() {

        $("#tree2").fancytree({
            source: {
                url: "/tree/datasets"
            }
        });

        $("#tree1").fancytree({
            source: {
                url: "/tree/metrics"
            },
            lazyLoad: function(event, data){
                var node = data.node;
                var url = '#';
                if (node.data.level == 1)
                {
                    url = "/tree/metric/" + node.title;
                }
                else if (node.data.level == 2)
                {
                    url = "/tree/metric/" + node.data.parent + '/' + node.title;
                }
                data.result = {
                    url: url,
                    cache: false
                };
            }
        });

        $("#tree3").fancytree({
            source: {
                url: "/tree/flows"
            },
            lazyLoad: function(event, data){
                var node = data.node;
                var url = '#';
                if (node.data.level == 1)
                {
                    url = "/tree/flow/" + node.title;
                }
                else if (node.data.level == 2)
                {
                    url = "/tree/flow/" + node.data.parent + '/' + node.title;
                }
                data.result = {
                    url: url,
                    cache: false
                };
            }
        });

        $("#tree2").bind("fancytreeactivate", function(event, data){

            var node = data.node;
            if(node)
            {
                if (node.isFolder())
                {
                    window.location = "#/datasets/name/" + node.title + "/page/1?urn=" + node.data.path;
                }
                else{
                    if (node && node.data && node.data.id)
                    {
                        window.location = "#/datasets/" + node.data.id;
                    }
                }
            }
        });

        $("#tree2").bind("fancytreeinit", function(event, data){
            if (window.g_currentDatasetNodeName && window.g_currentDatasetNodeUrn)
            {
                findAndActiveDatasetNode(window.g_currentDatasetNodeName, window.g_currentDatasetNodeUrn);
            }
            window.g_currentDatasetNodeName = null;
            window.g_currentDatasetNodeUrn = null;
        });

        $("#tree3").bind("fancytreeactivate", function(event, data){
            var node = data.node;
            if(node)
            {
                if (node.data.level == 1)
                {
                    window.location = "#/flows/" + node.title + "/page/1";
                }
                else if(node.data.level == 2)
                {
                    window.location = "#/flows/" + node.parent.title + '/' + node.title + "/page/1";
                }
                else if(node.data.level == 3)
                {
                    window.location = "#/flows/" + node.parent.parent.title + '/' + node.parent.title + '/' + node.data.id + "/page/1";
                }
            }
        });


        $("#tree3").bind("fancytreeinit", function(event, data){
            if (window.g_currentFlowApplication)
            {
                findAndActiveFlowNode(window.g_currentFlowApplication,
                    window.g_currentFlowProject,
                    window.g_currentFlowId,
                    window.g_currentFlowName);
            }
            window.g_currentFlowApplication = null;
            window.g_currentFlowProject = null;
            window.g_currentFlowName = null;
            window.g_currentFlowId = null;
        });

        $("#tree1").bind("fancytreeactivate", function(event, data){
            var node = data.node;
            if(node)
            {
                if (node.isFolder())
                {
                    if (node.data && node.data.level)
                    {
                        var level = node.data.level;
                        if (level == 1)
                        {
                            window.location = "#/metrics/name/" + node.title + '/page/1';
                        }
                        else if (level == 2)
                        {
                            if (node.parent && node.parent.title)
                            {
                                window.location = "#/metrics/name/" +
                                    node.parent.title + '/' +
                                    node.title +
                                    '/page/1';
                            }
                        }
                    }
                }
                else{
                    if (node && node.data && node.data.id)
                    {
                        window.location = "#/metrics/" + node.data.id;
                    }
                }
            }
        });

        $("#tree1").bind("fancytreeinit", function(event, data){

            if (window.g_currentMetricNodeName && window.g_currentMetricNodeId)
            {
                findAndActiveMetricNode(window.g_currentMetricNodeName,
                    window.g_currentMetricNodeId);
            }
            else if (window.g_currentMetricDashboardName && window.g_currentMetricGroupName)
            {
                findAndActiveMetricGroupNode(window.g_currentMetricDashboardName,
                    window.g_currentMetricGroupName);
            }
            else if (window.g_currentMetricDashboardName)
            {
                findAndActiveMetricDashboardNode(window.g_currentMetricDashboardName);
            }
            window.g_currentMetricNodeName = null;
            window.g_currentMetricNodeId = null;
            window.g_currentMetricDashboardName = null;
            window.g_currentMetricGroupName = null;
        });
    });

})(window, jQuery)
