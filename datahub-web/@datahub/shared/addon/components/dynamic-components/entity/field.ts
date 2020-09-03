import Component from '@glimmer/component';
import { DataModelEntity } from '@datahub/data-models/constants/entity';

interface IDynamicComponentsEntityFieldArgs {
  // The related entity to this dynamic component
  entity: DataModelEntity;
  options: {
    className: string;
    // Intended to be the field of the entity by key that we want to render the field value for
    fieldName: string;
  };
}

/**
 * The intention of this component is to, given an entity that is presumably passed in and a field name, to render the
 * field or yield that value in a render props friendly way
 */
export default class DynamicComponentsEntityField extends Component<IDynamicComponentsEntityFieldArgs> {}
