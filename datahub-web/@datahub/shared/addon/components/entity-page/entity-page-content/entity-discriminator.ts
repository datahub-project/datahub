import Component from '@glimmer/component';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';
import { get } from '@ember/object';
import { IEntityDiscriminator } from '@datahub/data-models/types/entity/rendering/page-components';

/**
 * Arguments for this component
 */
interface IEntityPageContentEntityDiscriminatorArgs<T> {
  // options for this component
  options: IEntityDiscriminator<T>['options'];
  // urn of dataset to extract platform
  entity: T;
}

/**
 * This component will render a component based on the value of a property in an entity.
 *
 * for example:
 *
 * entity1 = {
 *   type: 'GRAPHQL'
 * };
 *
 * entity2 = {
 *  type = 'PEGASUS'
 * };
 *
 * {
 *   discriminator: {
 *     GRAPHQL: ifGraphQLRenderThisComponent
 *   },
 *   default: forOtherCasesLikePEGASUSRenderThisComponent,
 *   propertyName: 'type'
 * }
 *
 */
export default class EntityPageContentEntityDiscriminator<T> extends Component<
  IEntityPageContentEntityDiscriminatorArgs<T>
> {
  /**
   * Selected component base on the value of a property. If the value is undefined or not mapped,
   * we will use default.
   *
   * @readonly
   */
  get component(): IDynamicComponent {
    const { entity, options } = this.args;
    const { discriminator, default: defaultComponent, propertyName } = options;
    const propertyValue = (get(entity, propertyName) as unknown) as Extract<T[keyof T], string>;
    const discriminatedComponent: IDynamicComponent | undefined = propertyValue && discriminator[propertyValue];
    return discriminatedComponent || defaultComponent;
  }
}
