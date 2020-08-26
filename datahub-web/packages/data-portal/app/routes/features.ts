import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';

/**
 * Defines the top-level route class for Features
 * @export
 * @class Features
 * @extends {Route.extend(AuthenticatedRouteMixin)}
 */
export default class Features extends Route.extend(AuthenticatedRouteMixin) {}
