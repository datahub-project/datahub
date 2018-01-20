import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

export default Route.extend(AuthenticatedRouteMixin, {
  setupController: function(controller) {
    const selectedUser = {
      userId: 'jweiner',
      displayName: 'jweiner',
      headlessTicketsCompletion: 0,
      totalHeadlessTickets: 0,
      openedHeadlessTickets: 0,
      closedHeadlessTickets: 0,
      cfCompletion: 0,
      cfTotalDatasets: 0,
      cfConfirmedDatasets: 0,
      url: '/metadata#/dashboard/jweiner'
    };
    let descriptionOptions = [
      { value: 'Has Dataset Description', option: 1 },
      { value: 'No Dataset Description', option: 2 },
      { value: 'Full Fields Description', option: 3 },
      { value: 'Has Fields Description', option: 4 },
      { value: 'No Fields Description', option: 5 },
      { value: 'All Datasets', option: 6 }
    ];
    let ownershipOptions = [
      { value: 'Confirmed Datasets', option: 1 },
      { value: 'Unconfirmed Datasets', option: 2 },
      { value: 'All Datasets', option: 3 }
    ];
    let idpcOptions = [
      { value: 'Auto Purge', option: 'AUTO_PURGE' },
      { value: 'Custome Purge', option: 'CUSTOM_PURGE' },
      { value: 'Limited Retention', option: 'LIMITED_RETENTION' },
      { value: 'Not Applicable', option: 'PURGE_NOT_APPLICABLE' },
      { value: 'Unknown', option: 'UNKNOWN' },
      { value: 'All Datasets', option: 'All Datasets' }
    ];

    controller.set('breadcrumbs', genBreadcrumbs('/jweiner'));
    controller.set('selectedUser', selectedUser);
    controller.set('descriptionOptions', descriptionOptions);
    controller.set('ownershipOptions', ownershipOptions);
    controller.set('idpcOptions', idpcOptions);
  }
});
