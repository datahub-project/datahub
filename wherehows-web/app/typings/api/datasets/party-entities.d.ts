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
