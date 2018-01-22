import Route from '@ember/routing/route';
import route from 'ember-redux/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { asyncRequestEntityQueryData } from 'wherehows-web/actions/browse/entity';

// TODO: DSS-6581 Create URL retrieval module
const listUrl = '/api/v1/list';
const queryParamsKeys = ['page', 'urn', 'name'];

/**
 * Creates a route handler for browse.entity route
 * entity can be any (datasets|metrics|flows)
 * @type {Ember.Route}
 */
const BrowseEntityRoute = Route.extend(AuthenticatedRouteMixin, {
  queryParams: queryParamsKeys.reduce(
    (queryParams, param) =>
      Object.assign({}, queryParams, {
        [param]: { refreshModel: true }
      }),
    {}
  )
});

/**
 * Ember Redux decorator wraps incoming route object and injects the redux store.dispatch method as the
 * first argument
 */
export default route({
  /**
   *
   * @param dispatch
   * @param params
   */
  model: (dispatch, params) => dispatch(asyncRequestEntityQueryData(params, listUrl, { queryParamsKeys }))
})(BrowseEntityRoute);
