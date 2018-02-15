import Route from '@ember/routing/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import Ember from 'ember'; // type import, no emit

export default class extends Route.extend(AuthenticatedRouteMixin) {
  afterModel(_model: any, transition: Ember.Transition & { params: any }) {
    // Extract the entity being viewed from the transition state
    const { params: { 'browse.entity': { entity = 'datasets' } = {} } } = transition;

    // transition to entity specific sub route
    this.transitionTo('browse.entity', entity);
  }
}
