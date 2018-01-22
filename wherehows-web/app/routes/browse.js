import Route from '@ember/routing/route';
import route from 'ember-redux/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { asyncRequestBrowseData } from 'wherehows-web/actions/browse';

// TODO: DSS-6581 Create URL retrieval module
// TODO: Route should transition to browse/entity, pay attention to the fact that
//   this route initializes store with entity metrics on entry
const entityUrls = {
  datasets: '/api/v1/datasets',
  metrics: '/api/v1/metrics',
  flows: '/api/v1/flows'
};

export default route({
  model: (dispatch, { entity = 'datasets', page = '1' }) => dispatch(asyncRequestBrowseData(page, entity, entityUrls))
})(
  Route.extend(AuthenticatedRouteMixin, {
    /**
     * Browse route does not render any content, but hydrates the store with initial data transition to child route
     * @param  {Object} model result from model call
     * @param {Ember.Transition} transition
     */
    afterModel(model, transition) {
      // Extract the entity being viewed from the transition state
      const { params: { 'browse.entity': { entity = 'datasets' } = {} } } = transition;
      this.transitionTo('browse.entity', entity);
    }
  })
);
