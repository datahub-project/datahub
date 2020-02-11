import EmberRouter from '@ember/routing/router';
import config from './config/environment';

const Router = EmberRouter.extend({
  location: config.locationType,
  rootURL: config.rootURL
});

Router.map(function() {
  this.route('wiki');
  this.route('testcomponents');

  this.route('user', function() {
    this.route('profile', { path: '/:user_id' }, function() {
      this.route('tab', { path: '/:tab_selected' });
    });
  });
});

export default Router;
