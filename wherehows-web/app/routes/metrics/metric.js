import Route from '@ember/routing/route';
import { setProperties } from '@ember/object';
import fetch from 'fetch';

const metricsUrlRoot = '/api/v1/metrics';

/**
 * Takes an object representing a metric and generates a list of breadcrumb items for each level in the
 *   hierarchy
 * @param {Object} metric properties for the current metric
 * @return {[*,*,*,*]}
 */
const makeMetricsBreadcrumbs = (metric = {}) => {
  let { id, dashboardName, group, category, name } = metric;
  dashboardName || (dashboardName = '(Other)');
  group || (group = '(Other)');
  name = category ? `{${category}} ${name}` : name;

  return [
    { crumb: 'Metrics', name: '' },
    { crumb: dashboardName, name: dashboardName },
    { crumb: group, name: `${dashboardName}/${group}` },
    { crumb: name, name: id }
  ];
};

export default Route.extend({
  setupController(controller, model) {
    const { metric } = model;

    // Set the metric as the model and create breadcrumbs
    setProperties(controller, {
      model: metric,
      breadcrumbs: makeMetricsBreadcrumbs(metric)
    });
  },

  /**
   * Fetches the metric with the id specified in the route
   * @param metric_id
   * @return {Promise<any>}
   */
  model({ metric_id }) {
    const metricsUrl = `${metricsUrlRoot}/${metric_id}`;
    return fetch(metricsUrl).then(response => response.json());
  }
});
