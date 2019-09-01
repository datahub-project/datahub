import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

/**
 * Defines the top-level route class for application Lists
 * @export
 * @class Lists
 * @extends {Route.extend(AuthenticatedRouteMixin)}
 */
export default class Lists extends Route.extend(AuthenticatedRouteMixin) {}
