import Ember from 'ember';

const {
  isEmpty,
  Service,
  $: { getJSON }
} = Ember;
const partyEntitiesUrl = '/api/v1/party/entities';

const cache = {
  // Cache containing results from the last request to partyEntities api
  partyEntities: null
};

/**
 * Async request for partyEntities. Caches the value from the initial request
 * @return {Promise.<Object|void>}
 */
const getLDAPUsers = () =>
  new Promise(resolve => {
    const cachedResults = cache.partyEntities;

    // Resolve with cachedResults if this has been previously requested
    if (!isEmpty(cachedResults)) {
      return resolve(cachedResults);
    }

    // Cast $.getJSON to native Promise
    Promise.resolve(getJSON(partyEntitiesUrl))
      .then(({ status, userEntities = [] }) => {
        if (status === 'ok' && userEntities.length) {
          /**
           * @type {Object} userEntitiesMaps hash of userEntities: label -> displayName
           */
          const userEntitiesMaps = userEntities.reduce(
            (map, { label, displayName }) => ((map[label] = displayName), map),
            {}
          );

          return ({
            userEntities,
            userEntitiesMaps,
            userEntitiesSource: Object.keys(userEntitiesMaps)
          });
        }
      })
      .then(results => cache.partyEntities = results)
      .then(resolve);
  });

/**
 * Takes a userNameQuery query and find userNames that match by starting with
 *  the pattern
 * @param {String} userNameQuery pattern to search for
 * @param {Function} syncResults callback
 * @param {Function} asyncResults callback
 */
const ldapResolver = (userNameQuery, syncResults, asyncResults) => {
  const regex = new RegExp(`^${userNameQuery}.*`, 'i');

  getLDAPUsers()
    .then(({ userEntitiesSource = {} }) =>
      userEntitiesSource.filter(entity => regex.test(entity)))
    .then(asyncResults);
};

/**
 * For a given userName, find the userEntity object that contains the userName
 * @param {String} userName the unique userName
 * @return {Promise.<TResult|null>} resolves with the userEntity or null otherwise
 */
const getPartyEntityWithUserName = userName => {
  return getLDAPUsers()
    .then(
      ({ userEntities }) => userEntities.find(({ label }) => label === userName) ||
      null
    );
};

export default Service.extend({
  getPartyEntityWithUserName,
  userNamesResolver: ldapResolver,
  fetchUserNames: getLDAPUsers
});
