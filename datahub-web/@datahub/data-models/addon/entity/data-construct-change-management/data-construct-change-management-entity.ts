import { BaseEntity, statics, IBaseEntityStatics } from '@datahub/data-models/entity/base-entity';
import { IEntityRenderProps } from '@datahub/data-models/types/entity/rendering/entity-render-props';

/**
 * A Placeholder entity only meant to get search working ( i.e have a valid displayName, renderProps )
 *
 * There is no intention to display an Entity page or utilize this as an aspect.
 */
@statics<IBaseEntityStatics<Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement>>()
export class DataConstructChangeManagementEntity extends BaseEntity<
  Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement
> {
  /**
   * Human friendly Identifier for the entity.
   */
  static displayName: 'data-construct-change-management' = 'data-construct-change-management';

  /**
   * Default renderProps for changeManagement.
   * We will not be dealing with entity pages for this entity.
   */
  static get renderProps(): IEntityRenderProps {
    return {
      apiEntityName: 'dataConstructChangeManagement',
      search: {
        placeholder: '',
        attributes: [],
        isEnabled: false,
        defaultAspects: []
      }
    };
  }
}
