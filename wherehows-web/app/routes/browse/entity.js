import Ember from 'ember';
import route from 'ember-redux/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { asyncRequestNodeList } from 'wherehows-web/actions/browse/entity';

const { Route } = Ember;

const listUrl = '/api/v1/list';
const queryParams = ['page', 'urn'];

const BrowseEntityRoute = Route.extend(AuthenticatedRouteMixin, {
  queryParams: queryParams.reduce(
    (queryParams, param) =>
      Object.assign({}, queryParams, {
        [param]: { refreshModel: true }
      }),
    {}
  )
});

export default route({
  model: (dispatch, params) => dispatch(asyncRequestNodeList(params, listUrl, { queryParams }))
})(BrowseEntityRoute);
