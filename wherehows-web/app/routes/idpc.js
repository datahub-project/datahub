import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

export default Route.extend(AuthenticatedRouteMixin, {
  setupController(controller) {
    const selectedUser = {
      userId: 'jweiner',
      displayName: 'jweiner',
      headlessTicketsCompletion: 0,
      totalHeadlessTickets: 0,
      openedHeadlessTickets: 0,
      closedHeadlessTickets: 0,
      userCompletion: 0,
      userTotalTickets: 0,
      userOpenTickets: 0,
      userClosedTickets: 0,
      url: '/idpc/jweiner'
    };
    const sortOptions = ['Assignee First', 'Jira Status First', 'Directory Path First'];

    controller.set('breadcrumbs', controller.get('genBreadcrumbs')('/jweiner'));
    controller.set('selectedUser', selectedUser);
    controller.set('sortOptions', sortOptions);
  }
});
