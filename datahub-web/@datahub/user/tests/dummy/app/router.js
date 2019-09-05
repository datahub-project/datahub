// NOTE: Named as .js to solve issue: no such file or directory, ...tests/dummy/app/router.js'
// when running ember g route <NAME> --dummy command
import EmberRouter from '@ember/routing/router';
import config from './config/environment';

const Router = EmberRouter.extend({
  location: config.locationType,
  rootURL: config.rootURL
});

Router.map(function() {
  this.route('user', function() {
    this.route('ump-flow');
    this.route(
      'entity',
      {
        path: '/entity/:entity'
      },
      function() {
        this.route('own');
      }
    );
    this.route('profile', { path: '/:user_id' });
  });
});

export default Router;
