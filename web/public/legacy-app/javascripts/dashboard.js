(function ($) {
    $(document).ready(function() {

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
            this.resource('dashboard', function(){
                this.resource('user', {path: '/:user'});
            });
        });

        App.IndexRoute = Ember.Route.extend({
            redirect: function() {
                this.transitionTo('user', "jweiner");
            }
        });

        var genBreadcrumbs = function(urn) {
            var breadcrumbs = []
            var b = urn.split('/')
            b.shift();
            for(var i = 0; i < b.length; i++) {
                var updatedUrn = "/metadata#/dashboard/" + b[i]
                if(i === 0)
                {

                    breadcrumbs.push({title: b[i], urn: updatedUrn})
                }
                else
                {
                    breadcrumbs.push({title: b[i], urn: updatedUrn})
                }
            }
            return breadcrumbs
        }

        function renderBarChart(obj, data, option)
        {
            if (!$(obj))
                return;

            $(obj).empty();
            var margin = {top: 20, right: 20, bottom: 30, left: 40},
                width = 500 - margin.left - margin.right,
                height = 300 - margin.top - margin.bottom;

            var label = 'dataset';
            if (option > 1 && option < 5)
            {
                label = 'field';
            }

            var x = d3.scale.ordinal()
                .rangeRoundBands([0, width], .1);

            var y = d3.scale.linear()
                .range([height, 0]);

            var xAxis = d3.svg.axis()
                .scale(x)
                .orient("bottom");

            var yAxis = d3.svg.axis()
                .scale(y)
                .orient("left")
                .ticks(10, "");

            var svg = d3.select(obj).append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
            x.domain(data.map(function(d) { return d.label; }));
            y.domain([0, d3.max(data, function(d) { return d.value; })]);

            svg.append("g")
                .attr("class", "x axis")
                .attr("transform", "translate(0," + height + ")")
                .call(xAxis);

            svg.append("g")
                .attr("class", "y axis")
                .call(yAxis)
                .append("text")
                .attr("y", 6)
                .attr("dy", ".71em")
                .style("text-anchor", "end")
                .text(label);

            svg.selectAll(".bar")
                .data(data)
                .enter().append("rect")
                .attr("class", "bar")
                .attr("x", function(d) { return x(d.label); })
                .attr("width", x.rangeBand())
                .attr("y", function(d) { return y(d.value); })
                .attr("height", function(d) { return height - y(d.value); });
        }

        var pie = [];
        var rendered= [];
        function renderPie(obj, description, value)
        {
            var currentUser = jiraController.get('currentDescriptionUser');
            var data = [
                    { label: description, value: value},
                    { label: "Other", value: currentUser.potentialDatasets - value}
                ];

            if (!rendered[obj])
            {
                pie[obj] = new d3pie(obj, {
                    size: {
                        canvasHeight: 250,
                        canvasWidth: 250,
                        pieInnerRadius: 0,
                        pieOuterRadius: null
                    },
                    labels: {
                        inner: {
                            format: "none"
                        }
                    },
                    data: {
                        content: [
                            { label: description, value: value},
                            { label: "Other", value: currentUser.potentialDatasets - value}
                        ]
                    },
                    labels: {
                        outer: {
                            format: "label",
                            hideWhenLessThanPercentage: null,
                            pieDistance: 20
                        },
                        inner: {
                            format: "percentage",
                            hideWhenLessThanPercentage: null
                        },
                        mainLabel: {
                            color: "#333333",
                            font: "arial",
                            fontSize: 8
                        },
                        percentage: {
                            color: "#dddddd",
                            font: "arial",
                            fontSize: 8,
                            decimalPlaces: 0
                        },
                        value: {
                            color: "#cccc44",
                            font: "arial",
                            fontSize: 8
                        },
                        lines: {
                            enabled: true,
                            style: "curved",
                            color: "segment" // "segment" or a hex color
                        }
                    },
                    tooltips: {
                        enabled: true,
                        type: "placeholder",
                        string: "{percentage}%",
                        styles: {
                            fadeInSpeed: 500,
                            backgroundColor: "#00cc99",
                            backgroundOpacity: 0.8,
                            color: "#ffffcc",
                            borderRadius: 4,
                            font: "verdana",
                            fontSize: 18,
                            padding: 18
                        }
                    }
                });
                rendered[obj] = true;
            }
            else
            {
                pie[obj].updateProp("data.content", data);
            }

        }

        var setActiveTab = function(){
            $('#dashboardtabs a:first').tab("show");
        }

        function refreshCfDatasets(user, page, size)
        {
            if (!user)
                return;

            if (!page)
                page = 1;
            if (!size)
                size = 10;
            var datasetsUrl = '/api/v1/metadata/dataset/confidential/' + user + '?page=' + page + '&size=' + size;
            $.get(datasetsUrl, function(data) {
                if (data && data.status == "ok") {
                    var currentPage = data.page;
                    var totalPage = data.totalPages;
                    if (currentPage == 1)
                    {
                        jiraController.set('cfFirst', true);
                    }
                    else
                    {
                        jiraController.set('cfFirst', false);
                    }
                    if (currentPage == totalPage)
                    {
                        jiraController.set('cfLast', true);
                    }
                    else
                    {
                        jiraController.set('cfLast', false);
                    }
                    jiraController.set('confidentialFieldsDatasets', data);
                    jiraController.set('currentCfPage', data.page);
                    if (data.datasets && data.datasets.length > 0)
                    {
                        jiraController.set('userNoConfidentialFields', false);
                    }
                    else
                    {
                        jiraController.set('userNoConfidentialFields', true);
                    }
                }
            });
        }

        function refreshOwnerDatasets(user, option, page, size, refresh)
        {
            if (!user)
                return;

            if (!page)
                page = 1;
            if (!size)
                size = 10;

            jiraController.set('ownerInProgress', true);
            var datasetsUrl = '/api/v1/metadata/dataset/ownership/' + user + '?page=' +
                page + '&size=' + size + '&option=' + option;

            $.get(datasetsUrl, function(data) {
                jiraController.set('ownerInProgress', false);
                if (data && data.status == "ok") {
                    var currentPage = data.page;
                    var totalPage = data.totalPages;
                    if (currentPage == 1)
                    {
                        jiraController.set('ownerFirst', true);
                    }
                    else
                    {
                        jiraController.set('ownerFirst', false);
                    }
                    if (currentPage == totalPage)
                    {
                        jiraController.set('ownerLast', true);
                    }
                    else
                    {
                        jiraController.set('ownerLast', false);
                    }
                    jiraController.set('ownershipDatasets', data);
                    if (data.datasets && data.datasets.length > 0)
                    {
                        if (refresh)
                        {
                            renderPie("ownerPie", ownershipOptions[option-1].value, data.count);
                        }
                        jiraController.set('userNoOwnershipFields', false);
                    }
                    else
                    {
                        jiraController.set('userNoOwnershipFields', true);
                    }
                }
            });

            if (refresh)
            {
                var barDataUrl = '/api/v1/metadata/barchart/ownership/' + user + '?option=' + option;
                $.get(barDataUrl, function(data) {
                    if (data && data.status == "ok") {
                        if (data.barData && data.barData.length > 0)
                        {
                            renderBarChart(('#ownerBarchart'), data.barData, option);
                        }
                    }
                });
            }
        }

        function refreshDescDatasets(user, option, page, size, refresh)
        {
            if (!user)
                return;

            if (!page)
                page = 1;
            if (!size)
                size = 10;
            jiraController.set('descInProgress', true);
            var datasetsUrl = '/api/v1/metadata/dataset/description/' + user + '?page=' +
                page + '&size=' + size + '&option=' + option;
            $.get(datasetsUrl, function(data) {
                jiraController.set('descInProgress', false);
                if (data && data.status == "ok") {
                    var currentPage = data.page;
                    var totalPage = data.totalPages;
                    if (currentPage == 1)
                    {
                        jiraController.set('descFirst', true);
                    }
                    else
                    {
                        jiraController.set('descFirst', false);
                    }
                    if (currentPage == totalPage)
                    {
                        jiraController.set('descLast', true);
                    }
                    else
                    {
                        jiraController.set('descLast', false);
                    }
                    jiraController.set('descriptionDatasets', data);
                    jiraController.set('currentDescPage', data.page);
                    if (data.datasets && data.datasets.length > 0)
                    {
                        if (refresh)
                        {
                            renderPie("pie", descriptionOptions[option-1].value, data.count);
                        }
                        jiraController.set('userNoDescriptionFields', false);
                    }
                    else
                    {
                        jiraController.set('userNoDescriptionFields', true);
                    }
                }
            });
            /*
            if (refresh)
            {
                var barDataUrl = '/api/v1/metadata/barchart/description/' + user + '?option=' + option;
                $.get(barDataUrl, function(data) {
                    if (data && data.status == "ok") {
                        if (data.barData && data.barData.length > 0)
                        {
                            renderBarChart(('#barchart'), data.barData, option);
                        }
                    }
                });
            }
            */
        }

        function refreshIdpcDatasets(user, option, page, size, refresh)
        {
            if (!user)
                return;

            if (!page)
                page = 1;
            if (!size)
                size = 10;

            jiraController.set('idpcInProgress', true);
            var datasetsUrl = '/api/v1/metadata/dataset/compliance/' + user + '?page=' +
                page + '&size=' + size + '&option=' + option;
            $.get(datasetsUrl, function(data) {
                jiraController.set('idpcInProgress', false);
                if (data && data.status == "ok") {
                    var currentPage = data.page;
                    var totalPage = data.totalPages;
                    if (currentPage == 1)
                    {
                        jiraController.set('idpcFirst', true);
                    }
                    else
                    {
                        jiraController.set('idpcFirst', false);
                    }
                    if (currentPage == totalPage)
                    {
                        jiraController.set('idpcLast', true);
                    }
                    else
                    {
                        jiraController.set('idpcLast', false);
                    }
                    jiraController.set('complianceDatasets', data);
                    jiraController.set('currentIdpcPage', data.page);
                    if (data.datasets && data.datasets.length > 0)
                    {
                        /*
                        if (refresh)
                        {
                            renderPie("pie", descriptionOptions[option-1].value, data.count);
                        }
                        */
                        jiraController.set('userNoComplianceFields', false);
                    }
                    else
                    {
                        jiraController.set('userNoComplianceFields', true);
                    }
                }
            });
            /*
            if (refresh)
            {
                var barDataUrl = '/api/v1/metadata/barchart/description/' + user + '?option=' + option;
                $.get(barDataUrl, function(data) {
                    if (data && data.status == "ok") {
                        if (data.barData && data.barData.length > 0)
                        {
                            renderBarChart(('#barchart'), data.barData, option);
                        }
                    }
                });
            }
            */
        }

        var jiraController = null;
        var hierarchy = '/jweiner';
        var breadcrumbs;
        var selectedUser = {
            'userId': 'jweiner',
            'displayName': 'jweiner',
            'headlessTicketsCompletion': 0,
            'totalHeadlessTickets': 0,
            'openedHeadlessTickets': 0,
            'closedHeadlessTickets': 0,
            'cfCompletion': 0,
            'cfTotalDatasets': 0,
            'cfConfirmedDatasets': 0,
            'url': '/metadata#/dashboard/jweiner'};
        var descriptionOptions = [
            {'value':'Has Dataset Description', 'option': 1},
            {'value':'No Dataset Description', 'option': 2},
            {'value':'Full Fields Description', 'option': 3},
            {'value':'Has Fields Description', 'option': 4},
            {'value':'No Fields Description', 'option': 5},
            {'value':'All Datasets', 'option': 6}];

        var ownershipOptions = [
            {'value':'Confirmed Datasets', 'option': 1},
            {'value':'Unconfirmed Datasets', 'option': 2},
            {'value':'All Datasets', 'option': 3}];

        var idpcOptions = [
            {'value':'Auto Purge', 'option': "AUTO_PURGE"},
            {'value':'Custome Purge', 'option': "CUSTOM_PURGE"},
            {'value':'Limited Retention', 'option': "LIMITED_RETENTION"},
            {'value':'Not Applicable', 'option': "PURGE_NOT_APPLICABLE"},
            {'value':'Unknown', 'option': "UNKNOWN"},
            {'value':'All Datasets', 'option': "All Datasets"}];

        setTimeout(setActiveTab, 500);

        App.DashboardRoute = Ember.Route.extend({
            setupController: function(controller) {
                jiraController = controller;
                breadcrumbs = genBreadcrumbs(hierarchy);
                jiraController.set('breadcrumbs', breadcrumbs);
                jiraController.set('selectedUser', selectedUser);
                jiraController.set('descriptionOptions', descriptionOptions);
                jiraController.set('ownershipOptions', ownershipOptions);
                jiraController.set('idpcOptions', idpcOptions);
            }
        });

        App.UserRoute = Ember.Route.extend({
            setupController: function(controller, params) {
                if (params && params.user)
                {
                    jiraController.set('ownerInProgress', true);
                    var ownershipUrl = 'api/v1/metadata/dashboard/ownership/' + params.user;
                    $.get(ownershipUrl, function(data) {
                        jiraController.set('ownerInProgress', false);
                        if (data && data.status == "ok") {
                            jiraController.set('ownershipMembers', data.members);
                            if (data.members && data.members.length > 0)
                            {
                                jiraController.set('userNoOwnershipMembers', false);
                            }
                            else
                            {
                                jiraController.set('userNoOwnershipMembers', true);
                            }
                            jiraController.set('currentOwnershipUser', data.currentUser);
                            var breadcrumbs;
                            if (data.currentUser.orgHierarchy)
                            {
                                breadcrumbs = genBreadcrumbs(data.currentUser.orgHierarchy);
                            }
                            else
                            {
                                var hierarchy = '/jweiner';
                                breadcrumbs = genBreadcrumbs(hierarchy);
                            }
                            jiraController.set('breadcrumbs', breadcrumbs);

                            var obj = $('#ownerShowOption');
                            if (obj)
                            {
                                refreshOwnerDatasets(params.user, obj.val(), 1, 10, true);
                            }
                            else
                            {
                                refreshOwnerDatasets(params.user, 1, 1, 10, true);
                            }
                        }
                    });

                    jiraController.set('cfInProgress', true);
                    var confidentialUrl = 'api/v1/metadata/dashboard/confidential/' + params.user;
                    $.get(confidentialUrl, function(data) {
                        jiraController.set('cfInProgress', false);
                        if (data && data.status == "ok") {
                            jiraController.set('confidentialFieldsOwners', data.members);
                            if (data.members && data.members.length > 0)
                            {
                                jiraController.set('userNoCfMembers', false);
                            }
                            else
                            {
                                jiraController.set('userNoCfMembers', true);
                            }
                            jiraController.set('currentConfidentialFieldsUser', data.currentUser);
                            var breadcrumbs;
                            if (data.currentUser.orgHierarchy)
                            {
                                breadcrumbs = genBreadcrumbs(data.currentUser.orgHierarchy);
                            }
                            else
                            {
                                var hierarchy = '/jweiner';
                                breadcrumbs = genBreadcrumbs(hierarchy);
                            }
                            jiraController.set('breadcrumbs', breadcrumbs);

                            refreshCfDatasets(params.user, 1, 10);
                        }
                    });

                    jiraController.set('descInProgress', true);
                    var descriptionUrl = 'api/v1/metadata/dashboard/description/' + params.user;
                    $.get(descriptionUrl, function(data) {
                        jiraController.set('descInProgress', false);
                        if (data && data.status == "ok") {
                            jiraController.set('descriptionOwners', data.members);
                            if (data.members && data.members.length > 0)
                            {
                                jiraController.set('userNoDescMembers', false);
                            }
                            else
                            {
                                jiraController.set('userNoDescMembers', true);
                            }
                            jiraController.set('currentDescriptionUser', data.currentUser);
                            var breadcrumbs;
                            if (data.currentUser.orgHierarchy)
                            {
                                breadcrumbs = genBreadcrumbs(data.currentUser.orgHierarchy);
                            }
                            else
                            {
                                var hierarchy = '/jweiner';
                                breadcrumbs = genBreadcrumbs(hierarchy);
                            }
                            jiraController.set('breadcrumbs', breadcrumbs);

                            var obj = $('#descShowOption');
                            if (obj)
                            {
                                refreshDescDatasets(params.user, obj.val(), 1, 10, true);
                            }
                            else
                            {
                                refreshDescDatasets(params.user, 1, 1, 10, true);
                            }
                        }
                    });

                    jiraController.set('idpcInProgress', true);
                    var complianceUrl = 'api/v1/metadata/dashboard/compliance/' + params.user;
                    $.get(complianceUrl, function(data) {
                        jiraController.set('idpcInProgress', false);
                        if (data && data.status == "ok") {
                            jiraController.set('complianceOwners', data.members);
                            if (data.members && data.members.length > 0)
                            {
                                jiraController.set('userNoIdpcMembers', false);
                            }
                            else
                            {
                                jiraController.set('userNoIdpcMembers', true);
                            }
                            jiraController.set('currentComplianceUser', data.currentUser);
                            var breadcrumbs;
                            if (data.currentUser.orgHierarchy)
                            {
                                breadcrumbs = genBreadcrumbs(data.currentUser.orgHierarchy);
                            }
                            else
                            {
                                var hierarchy = '/jweiner';
                                breadcrumbs = genBreadcrumbs(hierarchy);
                            }
                            jiraController.set('breadcrumbs', breadcrumbs);

                            var obj = $('#idpcShowOption');
                            if (obj)
                            {
                                refreshIdpcDatasets(params.user, obj.val(), 1, 10, true);
                            }
                            else
                            {
                                refreshIdpcDatasets(params.user, idpcOptions[0].value, 1, 10, true);
                            }
                        }
                    });
                }
            }
        });

        App.DashboardController = Ember.Controller.extend({
            cfFirst: false,
            cfLast: false,
            descFirst: false,
            descLast: false,
            actions: {
                prevOwnerPage: function() {
                    var cfInfo = this.get("ownershipDatasets");
                    var user = this.get("currentOwnershipUser");
                    if (cfInfo && user) {
                        var currentPage = parseInt(cfInfo.page) - 1;
                        if (currentPage > 0) {
                            refreshOwnerDatasets(user.userName, $('#ownerShowOption').val(), currentPage, 10, false);
                        }
                    }
                },
                nextOwnerPage: function() {
                    var cfInfo = this.get("ownershipDatasets");
                    var user = this.get("currentOwnershipUser");
                    if (cfInfo && user) {
                        var currentPage = parseInt(cfInfo.page) + 1;
                        var totalPages = cfInfo.totalPages;
                        if (currentPage <= totalPages) {
                            refreshOwnerDatasets(user.userName, $('#ownerShowOption').val(), currentPage, 10, false);
                        }

                    }
                },
                prevCfPage: function() {
                    var cfInfo = this.get("confidentialFieldsDatasets");
                    var user = this.get("currentConfidentialFieldsUser");
                    if (cfInfo && user) {
                        var currentPage = parseInt(cfInfo.page) - 1;
                        if (currentPage > 0) {
                            refreshCfDatasets(user.userName, currentPage, 10);
                        }
                    }
                },
                nextCfPage: function() {
                    var cfInfo = this.get("confidentialFieldsDatasets");
                    var user = this.get("currentConfidentialFieldsUser");
                    if (cfInfo && user) {
                        var currentPage = parseInt(cfInfo.page) + 1;
                        var totalPages = cfInfo.totalPages;
                        if (currentPage <= totalPages) {
                            refreshCfDatasets(user.userName, currentPage, 10);
                        }

                    }
                },
                prevDescPage: function() {
                    var descInfo = this.get("descriptionDatasets");
                    var user = this.get("currentDescriptionUser");
                    if (descInfo && user) {
                        var currentPage = parseInt(descInfo.page) - 1;
                        if (currentPage > 0) {
                            refreshDescDatasets(user.userName, $('#descShowOption').val(), currentPage, 10, false);
                        }
                    }
                },
                nextDescPage: function() {
                    var descInfo = this.get("descriptionDatasets");
                    var user = this.get("currentDescriptionUser");
                    if (descInfo && user) {
                        var currentPage = parseInt(descInfo.page) + 1;
                        var totalPages = descInfo.totalPages;
                        if (currentPage <= totalPages) {
                            refreshDescDatasets(user.userName, $('#descShowOption').val(), currentPage, 10, false);
                        }

                    }
                },
                prevIdpcPage: function() {
                    var idpcInfo = this.get("complianceDatasets");
                    var user = this.get("currentComplianceUser");
                    if (idpcInfo && user) {
                        var currentPage = parseInt(idpcInfo.page) - 1;
                        if (currentPage > 0) {
                            refreshIdpcDatasets(user.userName, $('#idpcShowOption').val(), currentPage, 10, false);
                        }
                    }
                },
                nextIdpcPage: function() {
                    var idpcInfo = this.get("complianceDatasets");
                    var user = this.get("currentComplianceUser");
                    if (idpcInfo && user) {
                        var currentPage = parseInt(idpcInfo.page) + 1;
                        var totalPages = idpcInfo.totalPages;
                        if (currentPage <= totalPages) {
                            refreshIdpcDatasets(user.userName, $('#idpcShowOption').val(), currentPage, 10, false);
                        }

                    }
                },
                optionChanged: function() {
                    var user = this.get("currentDescriptionUser");
                    if (user)
                    {
                        refreshDescDatasets(user.userName, $('#descShowOption').val(), 1, 10, true);
                    }
                },
                ownerOptionChanged: function() {
                    var user = this.get("currentOwnershipUser");
                    if (user)
                    {
                        refreshOwnerDatasets(user.userName, $('#ownerShowOption').val(), 1, 10, false);
                    }
                },
                idpcOptionChanged: function() {
                    var user = this.get("currentComplianceUser");
                    if (user)
                    {
                        refreshIdpcDatasets(user.userName, $('#idpcShowOption').val(), 1, 10, true);
                    }
                }
            }
        });
    });

})(jQuery)
