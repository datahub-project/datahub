import Ember from 'ember';
import connect from 'ember-redux/components/connect';

const { Component } = Ember;

/**
 * Matches string representing a url path segment that contains a `page` segment followed by a page number
 * The page number is retained
 * @type {RegExp}
 */
const pageRegex = /\/page\/([0-9]+)/i;
/**
 * Matches a url string with a `urn` query. urn query starts with letters or underscore segment of any length greater
 *   than 1 followed by colon and 3 forward slashes and a segment containing letters, _ or /, or none
 *   The value following the urn key is retained
 * @type {RegExp}
 */
const urnRegex = /urn=([a-z_]+[:\/]{3}[a-z_\/]*)/i;
/**
 * Matches a url string path segment that optionally starts with a hash followed by forward slash, at least one
 * alphabet, forward slash, number of varying length and optional trailing slash
 * The number is retained
 * @type {RegExp}
 */
const entityRegex = /^#?\/[a-z]+\/([0-9]+)\/?/i;

/**
 * Takes a node url and parses out the query params and path spec to be included in  the link component
 * @param {String} nodeUrl url linking to the node
 * @return {Object}
 */
const nodeUrlToQueryParams = nodeUrl => {
  const pageMatch = nodeUrl.match(pageRegex);
  const urnMatch = nodeUrl.match(urnRegex);
  let queryParams = null;

  // If we have a page match, append the page number to eventual urn object
  //  in most cases this page is usually, 1. Might be safe to remove the page pattern
  //  search in its entirety if this is the case
  if (Array.isArray(pageMatch)) {
    queryParams = Object.assign({}, queryParams, {
      page: pageMatch[1]
    });
  }

  // If we have a urn match, append the urn to eventual query params object
  if (Array.isArray(urnMatch)) {
    queryParams = Object.assign({}, queryParams, {
      urn: urnMatch[1]
    });
  }

  return queryParams;
};

/**
 * Selector function that takes a Redux Store to extract
 *   state props for the browser-rail
 * @param state
 * @return {{nodes: (*|Array)}}
 */
const stateToComputed = state => {
  // Extracts the current entity active in the browse view
  const { browseEntity: { currentEntity = '' } = {} } = state;
  // Retrieves properties for the current entity from the state tree
  const { [currentEntity]: { nodesByUrn = {}, query: { urn } = {} } } = state;
  // Removes `s` from the end of each entity name. Ember routes for individual entities are singular, and also
  //   returned entities contain id prop that is the singular type name, suffixed with Id, e.g. metricId
  // datasets -> dataset, metrics -> metric, flows -> flow
  const singularName = currentEntity.slice(0, -1);
  let nodes = nodesByUrn[urn] || [];
  /**
   * Creates dynamic query link params for each node
   * @type {Array} list of child nodes or datasets to render
   */
  nodes = nodes.map(({ nodeName, nodeUrl, [`${singularName}Id`]: id }) => {
    nodeUrl = String(nodeUrl);

    // If the id prop (datasetId|metricId|flowId) is truthy, then it is a standalone entity
    if (id) {
      return {
        title: nodeName,
        text: nodeName,
        route: `${currentEntity}.${singularName}`,
        model: nodeUrl.match(entityRegex)[1]
      };
    }

    return {
      title: nodeName,
      text: nodeName,
      route: `browse.entity`,
      model: currentEntity,
      queryParams: nodeUrlToQueryParams(nodeUrl)
    };
  });

  return { nodes };
};
export default connect(stateToComputed)(Component.extend({}));
