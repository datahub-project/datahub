import Component from '@glimmer/component';
import { IGridGroupEntity } from '@datahub/shared/types/grid-group';
import { IEntityListProfile } from '@datahub/shared/types/profile-list';

interface IGridGroupEntityProfileListArgs {
  /**
   * The list of external entities profiles rendered in this profile
   */
  profiles: Array<IEntityListProfile<IGridGroupEntity>>;
}

export const baseClass = 'grid-group-entity-profile-list';

/**
 * This component is a list of profiles for grid group entities.
 * Each profile will contain the grid group's name and an external link to the grid group entity page
 */
export default class GridGroupEntityProfileList extends Component<IGridGroupEntityProfileListArgs> {
  // Declared for convenient access in the template
  baseClass = baseClass;
}
