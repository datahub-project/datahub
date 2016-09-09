(function ($) {
    $(document).ready(function() {

        var obj = $("#mainnavbar .nav").find(".active");
        if (obj)
        {
            obj.removeClass("active");
        }
        $('#toolslink').addClass("active");

        App = Ember.Application.create({rootElement: "#content"});

        if (Ember.Debug && typeof Ember.Debug.registerDeprecationHandler === 'function') {
            Ember.Debug.registerDeprecationHandler(function(message, options, next) {
                if (options && options.id && options.id == 'ember-routing.router-resource') {
                    return;
                }
                next(message, options);
            });
        }

        App.Router.map(function() {
            this.resource('schemas', function(){
                this.resource('page', {path: '/page/:page'});
                this.resource('schema', {path: '/:id'});
            });
        });

        var schemaName = $("#name").val();


        var instance = jsondiffpatch.create({
            objectHash: function(obj, index) {
                if (typeof obj._id !== 'undefined') {
                    return obj._id;
                }
                if (typeof obj.id !== 'undefined') {
                    return obj.id;
                }
                if (typeof obj.name !== 'undefined') {
                    return obj.name;
                }
                return '$$index:' + index;
            }
        });

        var currentLeft, currentRight, leftSelected, rightSelected;
        var chartData = [];
        var schemaData = [];
        var skipChangeEvent = false;

        function updateSchemas(page, datasetId)
        {
            var url;
            if (!schemaName)
            {
                url = '/api/v1/schemaHistory/datasets?size=10&page=' + page;
            }
            else
            {
                url = '/api/v1/schemaHistory/datasets?name=' + schemaName + '&size=10&page=' + page;
            }

            if (datasetId && datasetId > 0)
            {
                url += '&datasetId=' + datasetId;
            }

            $.get(url, function(data) {
                if (data && data.status == "ok"){
                    if (schemasController)
                    {
                        schemasController.set('model', data);
                        if (data.data && data.data.datasets && data.data.datasets.length > 0)
                        {
                            updateTimeLine(data.data.datasets[0].id, true);
                        }
                    }
                }
            });
        }

        $("#name").bind("paste keyup", function() {
            schemaName = $("#name").val();
            updateSchemas(1, 0);
        });

        function updateDiffView()
        {
            var delta = instance.diff(currentLeft, currentRight);
            $("#schemaContent").html(jsondiffpatch.formatters.html.format(delta, currentLeft));
            jsondiffpatch.formatters.html.hideUnchanged();
        }

        function updateTimeLine(id, highlightFirstRow)
        {
            var historyUrl = '/api/v1/schemaHistory/historyData/' + id;
            $.get(historyUrl, function(data) {
                if(data && data.status == "ok")                {

                    $('#historytabs a:first').tab("show");
                    if (highlightFirstRow)
                    {
                        highlightRow(null, null, true);
                    }
                    schemaData = data.data;
                    currentRight = JSON.parse(schemaData[schemaData.length-1].schema);
                    currentLeft = {};
                    rightSelected = schemaData[schemaData.length-1];
                    leftSelected = null;
                    updateDiffView();
                    chartData = [];
                    $("#leftSchemaSelector").html('');
                    $("#leftSchemaSelector").append(new Option('-- choose a date --', 'na'));
                    $("#rightSchemaSelector").html('');
                    $("#rightSchemaSelector").append(new Option('-- choose a date --', 'na'));
                    for(var i = 0; i < schemaData.length; i++)
                    {
                        var modified = data.data[i].modified;
                        $("#leftSchemaSelector").append(new Option(modified, i));
                        $("#rightSchemaSelector").append(new Option(modified, i));
                        var fields = parseInt(schemaData[i].fieldCount);
                        var dateArray = modified.split("-")
                        if (dateArray && dateArray.length == 3)
                        {
                            chartData.push([Date.UTC(dateArray[0], dateArray[1] - 1, dateArray[2]), fields])
                        }
                    }
                    $('#leftSchemaSelector').change(function(){
                        if (skipChangeEvent)
                        {
                            return;
                        }
                        var selected = $('#leftSchemaSelector').val();
                        if (selected == 'na'){
                            currentLeft = {};
                        } else {
                            var index = parseInt(selected);
                            var left = JSON.parse(schemaData[selected].schema);
                            currentLeft = left;
                        }
                        updateDiffView();
                    });

                    $('#rightSchemaSelector').change(function(){
                        if (skipChangeEvent)
                        {
                            return;
                        }
                        var selected = $('#rightSchemaSelector').val();
                        if (selected == 'na'){
                            currentRight = {};
                        } else {
                            var index = parseInt(selected);
                            var right = JSON.parse(schemaData[selected].schema);
                            currentRight = right;
                        }
                        updateDiffView();
                    });
                    $('#rightSchemaSelector').val((schemaData.length-1).toString());
                    $('#leftSchemaSelector').val('na');
                    $('#timeline').highcharts({
                        title:
                        {
                            text: 'Schema History',
                            x: -20 //center
                        },
                        xAxis:
                        {
                            type: 'datetime',
                            dateTimeLabelFormats: { // don't display the dummy year
                                month: '%b  %Y',
                                year: '%Y'
                            }
                        },
                        yAxis: {
                            labels: {
                                enabled: true
                            },
                            title: {
                                text: 'Column number'
                            }
                        },
                        plotOptions: {
                            series: {
                                cursor: 'pointer',
                                events: {
                                    click: function (event) {
                                        skipChangeEvent = true;
                                        if (event && event.point)
                                        {
                                            var index = event.point.index;
                                            if (index)
                                            {
                                                $('#rightSchemaSelector').val(index.toString());
                                                if (index > 0)
                                                {
                                                    $('#leftSchemaSelector').val((index-1).toString());
                                                }
                                                currentRight = JSON.parse(schemaData[index].schema);
                                                currentLeft = JSON.parse(schemaData[index-1].schema);
                                                updateDiffView();
                                            }
                                            else if (index == 0)
                                            {
                                                $('#rightSchemaSelector').val(index.toString());
                                                $('#leftSchemaSelector').val('na');
                                                currentRight = JSON.parse(schemaData[index].schema);
                                                currentLeft = {};
                                                updateDiffView();
                                            }
                                        }
                                        skipChangeEvent = false;

                                        $('#historytabs a:last').tab("show");
                                        /*
                                         var obj = $( "a[tab-heading-transclude='']");
                                         if (obj && obj.length > 1)
                                         {
                                         $(obj[1]).click();
                                         }
                                         */
                                    }
                                }
                            }
                        },
                        tooltip: {
                            formatter: function() {
                                var index = this.point.index;
                                var changed = 0;
                                var text = "<b>" + Highcharts.dateFormat('%b %e %Y', this.point.x) + '</b><br/>'
                                if (index == 0)
                                {
                                    text += 'since last change <br/><span style="color:blue;' +
                                    'text-decoration: underline;font-style: italic;">';
                                    text+= 'Click the node to view schema</span>';
                                    return text;
                                }
                                else
                                {
                                    changed = schemaData[index].fieldCount - schemaData[index-1].fieldCount;
                                }

                                if (changed == 0)
                                {
                                    text += "No";
                                }
                                else{
                                    text += Math.abs(changed);
                                }

                                if (changed == 1 || changed == -1) {
                                    text += ' column has been ';
                                }
                                else {
                                    text += ' columns has been ';
                                }
                                if (changed == 0)
                                {
                                    text += ' added/removed ';
                                }
                                else if (changed > 0)
                                {
                                    text += ' added ';
                                }
                                else {
                                    text += ' removed ';
                                }
                                text += 'since last change <br/><span style="color:blue;' +
                                'font-style: italic;">';
                                text+= 'Click the node to view diff</span>';

                                return text;
                            }
                        },
                        credits: {
                            enabled: false
                        },
                        legend: {
                            layout: 'vertical',
                            align: 'right',
                            verticalAlign: 'middle',
                            borderWidth: 0
                        },
                        series: [{
                            showInLegend: false,
                            data: chartData
                        }]
                    });
                }
            });
        }

        function highlightRow(dataset, data, firstRow)
        {
            var rows = $(".schema-row");
            if (rows)
            {
                if (firstRow)
                {
                    $(rows[0]).addClass('highlight');
                    return;

                }
                for(var index = 0; index < data.data.datasets.length; index++)
                {
                    if (dataset == data.data.datasets[index])
                    {
                        $(rows[index]).addClass('highlight').siblings().removeClass('highlight');
                        break;

                    }
                }
            }
        }

        $("#name").bind("paste keyup", function() {
            schemaName = $("#name").val();
            updateSchemas(1, 0);
        });

        App.Router.reopen({
            rootURL: '/schemas/page/1'
        });

        App.IndexRoute = Ember.Route.extend({
            redirect: function() {
                this.transitionTo('page', 1);
            }
        });

        var schemasController = null;

        App.SchemasRoute = Ember.Route.extend({
            setupController: function(controller) {
                schemasController = controller;
            }
        });

        App.PageRoute = Ember.Route.extend({
            setupController: function(controller, params) {
                updateSchemas(params.page, 0);
            }
        });

        App.SchemaRoute = Ember.Route.extend({
            setupController: function(controller, params) {
                updateSchemas(1, params.id);
            }
        });

        App.SchemasController = Ember.Controller.extend({
            actions: {
                onSelect: function(dataset, data) {
                    highlightRow(dataset, data, false);
                    if (dataset && (dataset.id != 0))
                    {
                        updateTimeLine(dataset.id, false);
                    }
                }
            },
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

        App.SchemaController = Ember.Controller.extend({
            actions: {
                onSelect: function(dataset, data) {
                    highlightRow(dataset, data, false);
                    if (dataset && (dataset.id != 0))
                    {
                        updateTimeLine(dataset.id, false);
                    }
                }
            },
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
                }
                else {
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
                }
                else {
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
                        return false;
                    }
                }
                else {
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
                        return false;
                    }
                }
                else {
                    return false;
                }
            }.property('model.data.page')
        });
    });


})(jQuery)
