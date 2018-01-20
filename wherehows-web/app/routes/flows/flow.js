import Route from '@ember/routing/route';
import { setProperties } from '@ember/object';
import fetch from 'fetch';

const flowUrlRoot = 'api/v1/flow';

/**
 * Takes an object representing a flow and generates a list of breadcrumb items for navigating this hierarchy
 * @param {Object} props properties for the current flow
 * @return {[*,*,*]}
 */
const makeFlowsBreadcrumbs = (props = {}) => {
  const { name: application, id, flow } = props;
  return [
    { crumb: 'Flows', name: '', urn: '' },
    { crumb: application, name: application, urn: '' },
    { crumb: flow, flow_id: id, urn: application }
  ];
};

export default Route.extend({
  setupController: function(controller, model = {}) {
    setProperties(controller, {
      model,
      breadcrumbs: makeFlowsBreadcrumbs(model.data)
    });
  },

  model: async (params = {}) => {
    const { flow_id, name } = params;
    const flowsUrl = `${flowUrlRoot}/${name}/${flow_id}`;
    let response = await fetch(flowsUrl).then(response => response.json());

    if (response && response.status === 'ok') {
      response.data = Object.assign({}, response.data, {
        name
      });
    }

    return response;
  }
});
