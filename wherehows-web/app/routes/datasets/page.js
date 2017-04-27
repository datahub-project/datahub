import Ember from 'ember';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import route from 'ember-redux/route';
import { lazyRequestPagedDatasets } from 'wherehows-web/actions/datasets';

const {
  Route
} = Ember;

// TODO: DSS-6581 Create URL retrieval module
const datasetsPageBaseURL = '/api/v1/datasets?size=10&page=';
const DatasetsPageRoute = Route.extend(AuthenticatedRouteMixin);

export default route({
  model: (dispatch, { page = 1 }) => dispatch(lazyRequestPagedDatasets({ baseURL, page }))
})(DatasetsPageRoute);
