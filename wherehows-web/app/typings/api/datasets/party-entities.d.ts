import { ApiStatus } from 'wherehows-web/utils/api/shared';

/**
 * Describes the interface for a party entity
 */
export interface IPartyEntity {
  category: string;
  displayName: string;
  label: string;
}

/**
 * Describes the expected shape of the response for party entities endpoint
 */
export interface IPartyEntityResponse {
  status: ApiStatus;
  userEntities?: Array<IPartyEntity>;
}

/**
 * Describes a userEntityMap interface
 */
export interface userEntityMap {
  [label: string]: string;
}

/**
 * Describes the props resolved by the getUserEntities function
 */
export interface IPartyProps {
  userEntities: Array<IPartyEntity>;
  userEntitiesMaps: userEntityMap;
  userEntitiesSource: Array<keyof userEntityMap>;
}
