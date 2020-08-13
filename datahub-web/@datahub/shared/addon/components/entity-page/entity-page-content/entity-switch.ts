import Component from '@glimmer/component';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { IEntitySwitch } from '@datahub/data-models/types/entity/rendering/page-components';

interface IEntityPageContentEntitySwitchArgs<E> {
  // original entity where to fetch the new entity
  entity: DataModelEntityInstance;
  // contains the propertyName and child component to render
  options: IEntitySwitch<E>['options'];
}

/**
 * This component will switch the entity passed down to components to another one
 * using a property of the original entity. This way we can reuse components meant for entity1,
 * on entity2 if there is a relation.
 */
export default class EntityPageContentEntitySwitch<E> extends Component<IEntityPageContentEntitySwitchArgs<E>> {}
