import { actorUrnBasePrefix } from '@datahub/data-models/config/urn/actor';

/**
 * Returns an actor (username) from a user's urn.
 * @param urn - expected to be a urn for a user
 */
export const getActorFromUrn = (urn: string): string => {
  if (typeof urn !== 'string') {
    return urn;
  }

  return urn.replace(actorUrnBasePrefix, '');
};

export default getActorFromUrn;
