import EmberRouter from '@ember/routing/router';
import config from './config/environment';

export default class Router extends EmberRouter {
  location = config.locationType;
  rootURL = config.rootURL;
}

Router.map(function() {
  this.route('data-access');

  this.route('user', function() {
    this.route('ump-flow');
    this.route(
      'entity',
      {
        path: '/entity/:entity'
      },
      function() {
        this.route('own');
        this.route('access');
      }
    );
    this.route('profile', { path: '/:user_id' });
  });
});
