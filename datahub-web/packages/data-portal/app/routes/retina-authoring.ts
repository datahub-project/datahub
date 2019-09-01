import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

/**
 * Defines the top-level route class for RetinaAuthoring
 * @export
 * @class RetinaAuthoring
 * @extends {Route.extend(AuthenticatedRouteMixin)}
 */
export default class RetinaAuthoring extends Route.extend(AuthenticatedRouteMixin) {
  /**
   * Perform post model operations
   * @param params
   */
  afterModel(): void {
    // Route #/retina-authoring/ to /retina-authoring, thereby redirecting the user to the Authoring app.
    // This redirection however happens only post authentication through datahub.
    // [TODO] META-9668: Extend retina-authoring re-direction to child routes as well.
    window.location.replace(window.location.origin.concat('/retina-authoring/'));
  }
}
