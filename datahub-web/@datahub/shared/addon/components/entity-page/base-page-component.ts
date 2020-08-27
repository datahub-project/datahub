import { expandOptionsAttribute } from '@datahub/utils/decorators/expand-options-attribute';
import Component from '@ember/component';
import { assertComponentPropertyNotUndefined } from '@datahub/utils/decorators/assert';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';

@expandOptionsAttribute()
export default class BasePageComponent extends Component {
  @assertComponentPropertyNotUndefined
  entity!: DataModelEntityInstance;
}
