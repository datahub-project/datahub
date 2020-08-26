import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { IGridGroupEntity } from '@datahub/shared/types/grid-group';
import { IDynamicLinkParams } from 'dynamic-link/components/dynamic-link';

/**
 * The interface for an entity profile on an entity profile list
 *
 * @interface IEntityListProfile
 */
export interface IEntityListProfile<T = DataModelEntityInstance | IGridGroupEntity> {
  // The entity can either be an entity that is modeled on DataHub or externally supported
  entity: T;
  // The link params that redirects the user to the entity page on DataHub or an external link
  linkParam?: IDynamicLinkParams;
}
