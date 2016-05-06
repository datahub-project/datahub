(function ($) {
    $(document).ready(function() {

        App = Ember.Application.create({rootElement: "#content"});

        App.Router.map(function() {
            this.resource('scripts', function(){
                this.resource('page', {path: '/page/:page'});
                this.resource('script', {path: '/:jobID'});
            });
        });

        function getCategories(data) {
            if(!data) {
                return;
            }
            return data.map(function(x){
                return x.jobStarted
            })
        }
        var scriptTypes = '';
        var scriptName = $("#scriptName").val();
        var chainName = $("#chainName").val();
        function getSeries(data) {
            if(!data) {
                return [{}]
            }
            var count = data.length
            var tmp = {};
            for(var i = 0; i < count; i++) {
                var item = data[i];
                if(!tmp[item.jobPath]) {
                    tmp[item.jobPath] = {}
                    tmp[item.jobPath].name = item.jobPath
                    tmp[item.jobPath].data = []
                }
                tmp[item.jobPath].data.push(item.elapsedTime)
            }

            var tmp2 = [];
            for(var key in tmp) {
                tmp2.push(tmp[key])
            }
            return tmp2
        }

        function renderRuntimeHighcharts(data)
        {
            $('#runtime').highcharts({
                chart: {
                    type: 'bar'
                },
                title: {
                    text: 'Execution Time'
                },
                xAxis: {
                    categories: getCategories(data),
                    title:{
                        text: 'Start Time'
                    }
                },
                yAxis: {
                    title: {
                        text: 'Time (sec)'
                    }
                },
                tooltip: {
                    style: {
                        padding: 10,
                        fontWeight: 'bold'
                    }
                },
                plotOptions: {
                    bar: {
                        dataLabels: {
                            enabled: true
                        }
                    }
                },
                credits: {
                    enabled: false
                },
                series: getSeries(data)
            });
        }

        function updateScripts(page)
        {
            var url;

            var scriptNameObj = $('#scriptName');
            var scriptName = '';
            if (scriptNameObj)
            {
                scriptName = scriptNameObj.val();
            }

            var scriptPathObj = $('#scriptPath');
            var scriptPath = '';
            if (scriptPathObj)
            {
                scriptPath = scriptPathObj.val();
            }

            var scriptTypeObj = $('#scriptType');
            var scriptType = '';
            if (scriptTypeObj)
            {
                scriptType = scriptTypeObj.val();
            }

            var chainNameObj = $('#chainName');
            var chainName = '';
            if (chainNameObj)
            {
                chainName = chainNameObj.val();
            }

            var jobNameObj = $('#jobName');
            var jobName = '';
            if (jobNameObj)
            {
                jobName = jobNameObj.val();
            }

            var committerNameObj = $('#committerName');
            var committerName = '';
            if (committerNameObj)
            {
                committerName = committerNameObj.val();
            }

            var filterOpts = {};
            filterOpts.scriptName = scriptName;
            filterOpts.scriptPath = scriptPath;
            filterOpts.scriptType = scriptType;
            filterOpts.chainName = chainName;
            filterOpts.jobName = jobName;
            filterOpts.committerName = committerName;

            url = '/api/v1/scriptFinder/scripts?query=' + JSON.stringify(filterOpts) + '&size=10&page=' + page;

            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    scriptsController.set('model', data);
                    if(data.data && data.data.scripts && data.data.scripts.length > 0 && data.data.scripts[0])
                    {
                        var lineageUrl = '/api/v1/scriptFinder/scripts/lineage/' +
                            data.data.scripts[0].applicationID + '/' + data.data.scripts[0].jobID;
                        $.get(lineageUrl, function(data) {
                            if(data && data.status == "ok")
                            {
                                scriptsController.set('lineages', data.data);
                            }
                        });
                        var runtimeUrl = '/api/v1/scriptFinder/scripts/runtime/' +
                            data.data.scripts[0].applicationID + '/' + data.data.scripts[0].jobID;
                        $.get(runtimeUrl, function(data) {
                            if(data && data.status == "ok")
                            {
                                $("#scriptstable tr").eq(2).addClass('highlight').siblings().removeClass('highlight');
                                renderRuntimeHighcharts(data.data);
                            }
                        });

                    }
                }
            });
        }

        var scriptTypesUrl = '/api/v1/scriptFinder/scripts/types';

        function bindKeyEvent()
        {
            $(".script-filter").bind("paste keyup", function(){
                updateScripts(1);
            });
        }

        setTimeout(bindKeyEvent, 500);

        App.Router.reopen({
            rootURL: '/scripts/page/1'
        });

        App.IndexRoute = Ember.Route.extend({
            redirect: function() {
                this.transitionTo('page', 1);
            }
        });

        var totalScripts = null;
        var scriptsController = null;
        var scriptController = null;
        App.ScriptsRoute = Ember.Route.extend({
            setupController: function(controller) {
                scriptsController = controller;
            }
        });

        App.ScriptRoute = Ember.Route.extend({
            setupController: function(controller, params, transition) {
                //console.log(transition)
            }
        });

        App.PageRoute = Ember.Route.extend({
            setupController: function(controller, params, transition) {
                updateScripts(params.page);
            }
        });

        App.ScriptsController = Ember.Controller.extend({
            actions: {
                onSelect: function(script, data) {
                    if (script && script.applicationID && script.jobID)
                    {
                        var rows = $(".script-row");
                        if (rows)
                        {
                            for(var index = 0; index < data.data.scripts.length; index++)
                            {
                                if (script == data.data.scripts[index])
                                {
                                    $(rows[index+1]).addClass('highlight').siblings().removeClass('highlight');
                                    break;

                                }
                            }
                        }
                        var lineageUrl = '/api/v1/scriptFinder/scripts/lineage/' +
                            script.applicationID + '/' + script.jobID;
                        $.get(lineageUrl, function(data) {
                            if(data && data.status == "ok")
                            {
                                scriptsController.set('lineages', data.data);
                            }
                        });
                        var runtimeUrl = '/api/v1/scriptFinder/scripts/runtime/' +
                            script.applicationID + '/' + script.jobID;
                        $.get(runtimeUrl, function(data) {
                            if(data && data.status == "ok")
                            {
                                renderRuntimeHighcharts(data.data);
                            }
                        });
                    }
                },
                typeChanged: function(){
                    console.log($("#scriptTypeSelector").val());
                }

            },
            lineages: null,
            previousPage: function(){
                var model = this.get("model");
                if (model && model.data && model.data.page) {
                    var currentPage = model.data.page;
                    if (currentPage <= 1) {
                        return currentPage;
                    }
                    else {
                        return currentPage - 1;
                    }
                } else {
                    return 1;
                }

            }.property('model.data.page'),
            nextPage: function(){
                var model = this.get("model");
                if (model && model.data && model.data.page) {
                    var currentPage = model.data.page;
                    var totalPages = model.data.totalPages;
                    if (currentPage >= totalPages) {
                        return totalPages;
                    }
                    else {
                        return currentPage + 1;
                    }
                } else {
                    return 1;
                }
            }.property('model.data.page'),
            first: function(){
                var model = this.get("model");
                if (model && model.data && model.data.page) {
                    var currentPage = model.data.page;
                    if (currentPage <= 1) {
                        return true;
                    }
                    else {
                        return false
                    }
                } else {
                    return false;
                }
            }.property('model.data.page'),
            last: function(){
                var model = this.get("model");
                if (model && model.data && model.data.page) {
                    var currentPage = model.data.page;
                    var totalPages = model.data.totalPages;
                    if (currentPage >= totalPages) {
                        return true;
                    }
                    else {
                        return false
                    }
                } else {
                    return false;
                }
            }.property('model.data.page')
        });
    });

})(jQuery)
