import { BaseEntity, statics, IBaseEntityStatics } from '@datahub/data-models/entity/base-entity';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';
import { ICorpGroupResponse } from '@datahub/metadata-types/types/entity/group/group-entity';

/**
 * The GroupEntity represents a group of people entities, most often associated with headless
 * accounts or group aliases.
 */
@statics<IBaseEntityStatics<ICorpGroupResponse>>()
export class GroupEntity extends BaseEntity<ICorpGroupResponse> {
  /**
   * Identifier for the entity
   */
  static displayName: 'corp-groups' = 'corp-groups';

  /**
   * Class properties common across instances
   * Dictates how visual ui components should be rendered
   * Implemented as a getter to ensure that reads are idempotent
   * @readonly
   * @static
   */
  static get renderProps(): IEntityRenderProps {
    return {
      apiEntityName: 'corpgroup',
      search: {
        placeholder: '',
        attributes: [],
        isEnabled: false,
        defaultAspects: []
      }
    };
  }
}
