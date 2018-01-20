import Route from '@ember/routing/route';
import { set } from '@ember/object';
import $ from 'jquery';

export default Route.extend({
  controllerName: 'idpc',

  setupController(controller, params) {
    if (params && params.user) {
      controller.set('ticketsInProgress', true);
      var ticketsUrl = '/api/v1/jira/tickets/' + params.user;
      var headlessTickets;
      var userTickets;
      $.get(ticketsUrl, data => {
        controller.set('ticketsInProgress', false);
        if (data && data.status === 'ok') {
          controller.set('headlessTickets', data.headlessTickets);
          if (data.headlessTickets && data.headlessTickets.length > 0) {
            controller.set('headlessNoTickets', false);
            headlessTickets = data.headlessTickets;
            controller.set('headlessTickets', data.headlessTickets);
          } else {
            controller.set('headlessNoTickets', true);
          }
          controller.set('usersTickets', data.userTickets);
          if (data.userTickets && data.userTickets.length > 0) {
            controller.set('userNoTickets', false);
            userTickets = data.userTickets;
            controller.set('userTickets', data.userTickets);
          } else {
            controller.set('userNoTickets', true);
          }
          controller.set('membersInProgress', true);
          var membersUrl = '/api/v1/jira/members/' + params.user;
          $.get(membersUrl, data => {
            controller.set('membersInProgress', false);
            if (data && data.status === 'ok' && data.currentUser && data.currentUser[0]) {
              var currentUser = controller.get('selectedUser');
              set(currentUser, 'displayName', data.currentUser[0].displayName);
              var org = data.currentUser[0].orgHierarchy;
              var breadcrumbs;
              if (org) {
                breadcrumbs = controller.get('genBreadcrumbs')(org);
              } else {
                var hierarchy = '/jweiner';
                breadcrumbs = controller.get('genBreadcrumbs')(hierarchy);
              }
              controller.set('breadcrumbs', breadcrumbs);
              if (breadcrumbs) {
                set(currentUser, 'url', breadcrumbs[breadcrumbs.length - 1].urn);
              }

              if (data.members && data.members.length > 0) {
                controller.set('userNoMembers', false);
                var members = this.getMemberHeadlessTickets(data.members, headlessTickets);
                controller.set('members', members);
              } else {
                controller.set('userNoMembers', true);
              }
            }
          });
        }
      });
    }
  },

  getMemberHeadlessTickets(members, tickets) {
    if (!members) {
      return members;
    }
    var memberTotal = 0;
    var memberOpened = 0;
    var memberRollupTotal = 0;
    var memberRollupOpened = 0;

    for (var i = 0; i < members.length; i++) {
      memberTotal = 0;
      memberOpened = 0;
      if (tickets) {
        for (var j = 0; j < tickets.length; j++) {
          if (
            tickets[j].currentAssigneeOrgHierarchy &&
            tickets[j].currentAssigneeOrgHierarchy.indexOf(members[i].userName) !== -1
          ) {
            memberTotal += 1;
            if (tickets[j].ticketStatus.toLowerCase() === 'open') {
              memberOpened += 1;
            }
          }
        }
      }

      members[i].totalHeadlessTickets = memberTotal;
      members[i].openedHeadlessTickets = memberOpened;
      members[i].closedHeadlessTickets = memberTotal - memberOpened;
      if (members[i].totalHeadlessTickets > 0) {
        members[i].headlessTicketsCompletion = Math.round((memberTotal - memberOpened) / memberTotal * 100);
      } else {
        members[i].headlessTicketsCompletion = 100;
      }
      members[i].url = '/idpc/' + members[i].userName;
      memberRollupOpened += memberOpened;
      memberRollupTotal += memberTotal;
    }
    if (this.controller) {
      var currentUser = this.controller.get('selectedUser');
      var totalHeadlessTickets = memberRollupTotal;
      var openedHeadlessTickets = memberRollupOpened;
      var headlessTicketsCompletion;
      var closedHeadlessTickets = totalHeadlessTickets - openedHeadlessTickets;
      if (totalHeadlessTickets > 0) {
        headlessTicketsCompletion = Math.round(
          (totalHeadlessTickets - openedHeadlessTickets) / totalHeadlessTickets * 100
        );
      } else {
        headlessTicketsCompletion = 100;
      }
      set(currentUser, 'totalHeadlessTickets', totalHeadlessTickets);
      set(currentUser, 'openedHeadlessTickets', openedHeadlessTickets);
      set(currentUser, 'closedHeadlessTickets', closedHeadlessTickets);
      set(currentUser, 'headlessTicketsCompletion', headlessTicketsCompletion);
    }
    return members;
  }
});
