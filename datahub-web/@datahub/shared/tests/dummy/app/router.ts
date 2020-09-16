import EmberRouter from '@ember/routing/router';
import config from './config/environment';

export default class Router extends EmberRouter {
  location = config.locationType;
  rootURL = config.rootURL;
}

Router.map(function(): void {
  this.route('lineage-test');
  this.route('schema');
  this.route('browse');
  this.route('lineage', function() {
    this.route('urn', { path: '/:urn' });
  });
  this.route('wiki');
  this.route('testcomponents');
  // TODO: [META-11856] This is here as the acceptance test for the wiki tab throws an error because
  // this package has no concept of the user profile page, but the avatar links to such page. Should
  // be improved when we revamp how testing is done
  this.route('user', function() {
    this.route('profile', { path: '/:user_id' }, function() {
      this.route('tab', { path: '/:tab_selected' });
    });
  });
});
