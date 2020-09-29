import RouterDSL from '@ember/routing/-private/router-dsl';

export const sharedRoutes = (router: RouterDSL): void => {
  router.route('entity-type', { path: '/:entity_type' }, function(): void {
    this.route('urn', { path: '/:urn' }, function(): void {
      this.route('tab', { path: '/:tab_selected' });
    });
  });
};
