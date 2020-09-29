import Component from '@glimmer/component';
import { IEntityRenderCommonPropsSearch } from '@datahub/data-models/types/search/search-entity-render-prop';
import { DataModelName, DataModelEntityInstance, DataModelEntity } from '@datahub/data-models/constants/entity';
import { inject as service } from '@ember/service';
import DataModelsService from '@datahub/data-models/services/data-models';
import { alias } from '@ember/object/computed';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { tracked } from '@glimmer/tracking';

/**
 * Interface for the list of the arguments being fed into the recipient-search-select component
 */
interface IRecipientSearchSelectArgs {
  /**
   * The entity which resides as a property on the `data-construct-change-management` entity under `OwningEntity`
   */
  entity: DataModelEntityInstance;
  /**
   * Placeholder method that passes the action of adding a recipient back to the parent
   */
  addRecipient: (recipientType: string, recipientName: string) => void;
  /**
   * Placeholder method that passes the action of removing a recipient back to the parent
   */
  removeRecipient: (recipientType: string, recipientName: string, index: number) => void;
}

/**
 * A Dummy component that has no significant logic around API calls or data transformation but serves
 * as a wrapper for providing the required search configs and params.
 */
export default class RecipientSearchSelect extends Component<IRecipientSearchSelectArgs> {
  /**
   * Search config being passed over to leverage existing search container
   */
  searchConfig?: IEntityRenderCommonPropsSearch = {
    attributes: [],
    showFacets: false
  };

  /**
   * Number of search results we want to display in a single page.
   */
  pageSize = 5;

  /**
   * The search keyword being provided to dictate what aspect of the entity we search by
   * Here we escape the double quotes with a backslash to ensure encoding meets the search query requirements
   */
  @tracked
  keyword = '';

  /**
   * Get the entity whose display name we care about and whose instances we are searching for
   */
  get personEntity(): DataModelEntity {
    return this.dataModels.getModel('people');
  }

  /**
   * Injection of the data models service to access the general dataConstructChangeManagement entity class
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * The displayName of the entity whose instances we are searching for.
   * The category to narrow/ filter search results
   */
  @alias('personEntity.displayName')
  entityDisplayName: DataModelName = PersonEntity.displayName;
}
