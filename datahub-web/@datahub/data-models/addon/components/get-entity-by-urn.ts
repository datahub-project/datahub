import Component from '@glimmer/component';
import { inject as service } from '@ember/service';
import DataModelsService from '@datahub/data-models/services/data-models';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { extractEntityType } from '@datahub/utils/validators/urn';

interface IGetEntityByUrnArgs {
  urn: string;
}

/**
 * Allows us to get an entity instance from our data model service by its urn. Helpful when we are composing templates
 * and only have a urn to work with (and we may or may not know the entity type we are working with) and we want to
 * abstract the concern of instantiating a model with the data models service away from the parent component.
 *
 * @example
 * <GetEntityByUrn @urn="urn:li:dataset:someDataset" as |dataset|>
 *  <tr>
 *    <td>{{dataset.name}}</td>
 *    <td>{{dataset.someOtherInfo}}</td>
 *  </tr>
 * </GetEntityByUrn>
 */
export default class GetEntityByUrn extends Component<IGetEntityByUrnArgs> {
  /**
   * Injects the data models service so that we can get the actual entity class/instance
   */
  @service
  dataModels!: DataModelsService;

  /**
   * Given the entity type and urn, we are able to return a partial instance
   */
  get entityInstance(): DataModelEntityInstance | void {
    const urn = this.args.urn;
    const entityModel = this.dataModels.getModelByApiName(extractEntityType(urn) as string);
    return entityModel && this.dataModels.createPartialInstance(entityModel.displayName, urn);
  }
}
