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
});
