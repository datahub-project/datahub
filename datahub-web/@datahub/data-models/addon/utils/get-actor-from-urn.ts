import { corpUserUrnBasePrefix, actorUrnBasePrefix } from '@datahub/data-models/config/urn/base-prefix';

/**
 * Returns an actor (username) from a user's urn.
 * @param urn - expected to be a urn for a user
 */
export default (urn: string): string => urn.replace(corpUserUrnBasePrefix, '').replace(actorUrnBasePrefix, '');
