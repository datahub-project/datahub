import Ember from 'ember';
import route from 'ember-redux/route';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import { asyncRequestBrowseData } from 'wherehows-web/actions/browse';

const {
  Route
} = Ember;
// TODO: DSS-6581 Create URL retrieval module
const entityUrls = {
  datasets: '/api/v1/datasets?size=10&page=',
  metrics: '/api/v1/metrics?size=10&page=',
  flows: '/api/v1/flows?size=10&page='
};

export default route({
  model: (dispatch, { entity = 'datasets', page = '1' }) => dispatch(asyncRequestBrowseData(page, entity, entityUrls))
})(Route.extend(AuthenticatedRouteMixin));
