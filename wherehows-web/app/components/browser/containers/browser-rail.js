import Component from '@ember/component';
import { connect } from 'ember-redux';
import { urnRegex, specialFlowUrnRegex } from 'wherehows-web/utils/validators/urn';

/**
 * Matches string representing a url path segment that contains a `page` segment followed by a page number
 * The page number is retained
 * @type {RegExp}
 */
const pageRegex = /\/page\/([0-9]+)/i;
const nameRegex = /\/name\/([0-9a-z()_{}\[\]\/\s]+)/i;

/**
 * Takes a node url and parses out the query params and path spec to be included in  the link component
 * @param {String} nodeUrl url linking to the node
 * @return {Object}
 */
const nodeUrlToQueryParams = nodeUrl => {
  const pageMatch = nodeUrl.match(pageRegex);
  const urnMatch = nodeUrl.match(urnRegex);
  const flowUrnMatch = nodeUrl.match(specialFlowUrnRegex);
  const nameMatch = nodeUrl.match(nameRegex);
  let queryParams = null;

  // If we have a page match, append the page number to eventual urn object
  //  in most cases this page is usually, 1. Might be safe to remove the page pattern
  //  search in its entirety if this is the case
  if (Array.isArray(pageMatch)) {
    queryParams = Object.assign({}, queryParams, {
      page: pageMatch[1]
    });
  }

  if (Array.isArray(nameMatch)) {
    let match = nameMatch[1];
    match = match.split('/page')[0];

    queryParams = Object.assign({}, queryParams, {
      name: match
    });
  }

  // If we have a urn match, append the urn to eventual query params object
  if (Array.isArray(urnMatch) || Array.isArray(flowUrnMatch)) {
    const urn = urnMatch || [flowUrnMatch[1]];

    queryParams = Object.assign({}, queryParams, {
      // Extract the entire match as urn value
      urn: urn[0]
    });
  }

  return queryParams;
};

const getNodes = (entity, state = {}) => query =>
  ({
    get datasets() {
      return state[entity].nodesByUrn[query];
    },
    get metrics() {
      return state[entity].nodesByName[query];
    },
    get flows() {
      return this.datasets;
    }
  }[entity]);

/**
 * Selector function that takes a Redux Store to extract
 *   state props for the browser-rail
 * @param {Object} state current app state tree
 * @return {{nodes: (*|Array)}}
 */
const stateToComputed = state => {
  const header = 'Refine by';
  // Extracts the current entity active in the browse view
  const { browseEntity: { currentEntity = '' } = {} } = state;
  // Retrieves properties for the current entity from the state tree
  const { browseEntity: { [currentEntity]: { query: { urn, name } } } } = state;
  // Removes `s` from the end of each entity name. Ember routes for individual entities are singular, and also
  //   returned entities contain id prop that is the singular type name, suffixed with Id, e.g. metricId
  // datasets -> dataset, metrics -> metric, flows -> flow
  const singularName = currentEntity.slice(0, -1);
  const query =
    {
      datasets: urn,
      metrics: name,
      flows: urn
    }[currentEntity] || null;
  let nodes = getNodes(currentEntity, state)(query) || [];
  /**
   * Creates dynamic query link params for each node
   * @type {Array} list of child nodes or datasets to render
   */
  nodes = nodes.map(({ nodeName, nodeUrl, [`${singularName}Id`]: id, application = '' }) => {
    nodeUrl = String(nodeUrl);
    const node = {
      title: nodeName,
      text: nodeName
    };

    // If the id prop (datasetId|metricId|flowId) is truthy, then it is a standalone entity
    if (id) {
      let idNode = Object.assign({}, node);

      if (singularName === 'flow' && application) {
        idNode = Object.assign({}, idNode, {
          queryParams: {
            name: application
          }
        });
      }

      return Object.assign({}, idNode, {
        route: `${currentEntity}.${singularName}`,
        model: id
      });
    }

    return Object.assign({}, node, {
      route: `browse.entity`,
      model: currentEntity,
      queryParams: nodeUrlToQueryParams(nodeUrl)
    });
  });

  return { nodes, header };
};
export default connect(stateToComputed)(Component.extend({}));
