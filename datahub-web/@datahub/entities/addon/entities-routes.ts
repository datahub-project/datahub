import RouterDSL from '@ember/routing/-private/router-dsl';

export const entitiesRoutes = (router: RouterDSL): void => {
  router.route('user', function(): void {
    this.route('profile', { path: '/:user_id' }, function(): void {
      this.route('tab', { path: '/:tab_selected' });
    });
  });
  router.route('datasets', function(): void {
    this.route(
      'dataset',
      {
        path: '/:dataset_urn'
      },
      function(): void {
        this.route('tab', {
          path: '/:tab_selected'
        });
      }
    );
  });
};
