import Route from '@ember/routing/route';
import { isBlank } from '@ember/utils';
import $ from 'jquery';
import AuthenticatedRouteMixin from 'ember-simple-auth/mixins/authenticated-route-mixin';
import buildUrl from 'wherehows-web/utils/build-url';

const queryParams = ['keyword', 'category', 'page', 'source'];
// TODO: DSS-6581 Create URL retrieval module
const urlRoot = '/api/v1/search';

export default Route.extend(AuthenticatedRouteMixin, {
  // Set `refreshModel` for each queryParam to true
  //  so each url state change results in a full transition
  queryParams: queryParams.reduce((queryParams, param) => {
    queryParams[param] = { refreshModel: true };
    return queryParams;
  }, {}),

  /**
   * Applies the returned results object as the route model and sets
   *   keyword property on the route controller
   * @param {Object} controller search route controller
   * @param {Object} model search results
   */
  setupController(controller, model) {
    const { keywords } = model;

    controller.setProperties({
      model,
      keyword: keywords
    });
  },

  /**
   *
   * @param params
   */
  model(params = {}) {
    const searchUrl = queryParams.reduce((url, queryParam) => {
      const queryValue = params[queryParam];
      if (!isBlank(queryValue)) {
        return buildUrl(url, queryParam, queryValue);
      }

      return url;
    }, urlRoot);

    return Promise.resolve($.getJSON(searchUrl)).then(({ status, result }) => {
      if (status === 'ok') {
        const { keywords, data } = result;

        data.forEach((datum, index, data) => {
          const { schema } = datum;
          if (schema) {
            datum.originalSchema = schema;
            // TODO: DSS-6122 refactor global reference and function
            window.highlightResults(data, index, keywords);
          }
        });

        return result;
      }

      return Promise.reject(new Error(`Request for ${searchUrl} failed with: ${status}`));
    });
  }
});
