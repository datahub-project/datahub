(function ($) {
    $(document).ready(function() {

        App = Ember.Application.create({rootElement: "#content"});

        App.Router.map(function() {
            this.resource('jira', function(){
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
                var updatedUrn = "/idpc#/jira/" + b[i]
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
            $('#jiratabs a:first').tab("show");
        }

        var getMemberHeadlessTickets = function(members, tickets)
        {
            if (!members)
            {
                return members;
            }
            var memberTotal = 0;
            var memberOpened = 0;
            var memberRollupTotal = 0;
            var memberRollupOpened = 0;

            for(var i = 0; i < members.length; i++)
            {
                memberTotal = 0;
                memberOpened = 0;
                if (tickets)
                {
                    for(var j = 0; j < tickets.length; j++)
                    {
                        if (tickets[j].currentAssigneeOrgHierarchy &&
                          (tickets[j].currentAssigneeOrgHierarchy.indexOf(members[i].userName) != -1))
                        {
                            memberTotal += 1;
                            if (tickets[j].ticketStatus.toLowerCase() === 'open')
                            {
                                memberOpened += 1;
                            }
                        }
                    }
                }

                members[i].totalHeadlessTickets = memberTotal;
                members[i].openedHeadlessTickets = memberOpened;
                members[i].closedHeadlessTickets = memberTotal - memberOpened;
                if (members[i].totalHeadlessTickets > 0)
                {
                    members[i].headlessTicketsCompletion =
                        Math.round( ( (memberTotal - memberOpened) / memberTotal ) * 100 );
                }
                else
                {
                    members[i].headlessTicketsCompletion = 100;
                }
                members[i].url = "/idpc#/jira/" + members[i].userName;
                memberRollupOpened += memberOpened;
                memberRollupTotal += memberTotal;
            }
            if (jiraController)
            {
                var currentUser = jiraController.get('selectedUser');
                var totalHeadlessTickets = memberRollupTotal;
                var openedHeadlessTickets = memberRollupOpened;
                var headlessTicketsCompletion;
                var closedHeadlessTickets =
                    totalHeadlessTickets - openedHeadlessTickets;
                if (totalHeadlessTickets > 0)
                {
                    headlessTicketsCompletion =
                        Math.round( ( (
                        totalHeadlessTickets - openedHeadlessTickets) / totalHeadlessTickets ) * 100 );
                }
                else
                {
                  headlessTicketsCompletion = 100;
                }
                Ember.set(currentUser, 'totalHeadlessTickets', totalHeadlessTickets);
                Ember.set(currentUser, 'openedHeadlessTickets', openedHeadlessTickets);
                Ember.set(currentUser, 'closedHeadlessTickets', closedHeadlessTickets);
                Ember.set(currentUser, 'headlessTicketsCompletion', headlessTicketsCompletion);
            }
            return members;
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
            'userCompletion': 0,
            'userTotalTickets': 0,
            'userOpenTickets': 0,
            'userClosedTickets': 0,
            'url': '/idpc#/jira/jweiner'};
        setTimeout(setActiveTab, 500);

        App.JiraRoute = Ember.Route.extend({
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
                    var ticketsUrl = 'api/v1/jira/tickets/' + params.user;
                    var headlessTickets;
                    var userTickets;
                    $.get(ticketsUrl, function(data) {
                        jiraController.set('ticketsInProgress', false);
                        if (data && data.status == "ok") {
                            jiraController.set('headlessTickets', data.headlessTickets);
                            if (data.headlessTickets && data.headlessTickets.length > 0)
                            {
                                jiraController.set('headlessNoTickets', false);
                                headlessTickets = data.headlessTickets;
                                jiraController.set('headlessTickets', data.headlessTickets);
                            }
                            else
                            {
                                jiraController.set('headlessNoTickets', true);
                            }
                            jiraController.set('usersTickets', data.userTickets);
                            if (data.userTickets && data.userTickets.length > 0)
                            {
                                jiraController.set('userNoTickets', false);
                                userTickets = data.userTickets;
                                jiraController.set('userTickets', data.userTickets);
                            }
                            else
                            {
                                jiraController.set('userNoTickets', true);
                            }
                            jiraController.set('membersInProgress', true);
                            var membersUrl = 'api/v1/jira/members/' + params.user;
                            $.get(membersUrl, function(data) {
                                jiraController.set('membersInProgress', false);
                                if (data && data.status == "ok" && data.currentUser && data.currentUser[0]) {
                                    var currentUser = jiraController.get('selectedUser');
                                    Ember.set(currentUser, 'displayName', data.currentUser[0].displayName);
                                    var org = data.currentUser[0].orgHierarchy;
                                    var breadcrumbs;
                                    if (org)
                                    {
                                        breadcrumbs = genBreadcrumbs(org);
                                    }
                                    else
                                    {
                                        var hierarchy = '/jweiner';
                                        breadcrumbs = genBreadcrumbs(hierarchy);
                                    }
                                    jiraController.set('breadcrumbs', breadcrumbs);
                                    if (breadcrumbs)
                                    {
                                        Ember.set(currentUser, 'url', breadcrumbs[breadcrumbs.length-1].urn);
                                    }

                                    if (data.members && data.members.length > 0)
                                    {
                                        jiraController.set('userNoMembers', false);
                                        var members = getMemberHeadlessTickets(data.members, headlessTickets);
                                        jiraController.set('members', members);
                                    }
                                    else
                                    {
                                        jiraController.set('userNoMembers', true);
                                    }
                                }
                          });
                        }
                    });
                }
            }
        });

        App.JiraController = Ember.Controller.extend({
            actions: {
                onSelect: function(dataset, data) {
                    highlightRow(dataset, data, false);
                    if (dataset && (dataset.id != 0))
                    {
                        updateTimeLine(dataset.id, false);
                    }
                }
            }
        });
    });

})(jQuery)
