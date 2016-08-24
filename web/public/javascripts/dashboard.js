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

        var setActiveTab = function(){
            $('#dashboardtabs a:last').tab("show");
        }

        function refreshCfDatasets(user, page, size)
        {
            if (!user)
                return;

            if (!page)
                page = 1;
            if (!size)
                size = 10;
            var datasetsUrl = '/api/v1/metadata/dataset/' + user + '?page=' + page + '&size=' + size;
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


        var jiraController = null;
        var hierarchy = '/jweiner';
        var breadcrumbs;
        var sortOptions = ['Assignee First', 'Jira Status First', 'Directory Path First'];
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
        setTimeout(setActiveTab, 500);

        App.DashboardRoute = Ember.Route.extend({
            setupController: function(controller) {
                jiraController = controller;
                breadcrumbs = genBreadcrumbs(hierarchy);
                jiraController.set('breadcrumbs', breadcrumbs);
                jiraController.set('selectedUser', selectedUser);
                jiraController.set('sortOptions', sortOptions);
            }
        });

        App.UserRoute = Ember.Route.extend({
            setupController: function(controller, params) {
                if (params && params.user)
                {
                    jiraController.set('ticketsInProgress', true);
                    var confidentialUrl = 'api/v1/metadata/dashboard/' + params.user;
                    var headlessTickets;
                    var userTickets;
                    $.get(confidentialUrl, function(data) {
                        jiraController.set('ticketsInProgress', false);
                        if (data && data.status == "ok") {
                            jiraController.set('membersInProgress', true);
                            jiraController.set('confidentialFieldsOwners', data.members);
                            if (data.members && data.members.length > 0)
                            {
                                jiraController.set('userNoMembers', false);
                            }
                            else
                            {
                                jiraController.set('userNoMembers', true);
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
                }
            }
        });

        App.DashboardController = Ember.Controller.extend({
            cfFirst: false,
            cfLast: false,
            actions: {
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
                        if (currentPage < totalPages) {
                            refreshCfDatasets(user.userName, currentPage, 10);
                        }

                    }
                }
            }
        });
    });

})(jQuery)
