import Component from '@glimmer/component';
import { IEntityRenderCommonPropsSearch } from '@datahub/data-models/types/search/search-entity-render-prop';
import { DataModelName, DataModelEntityInstance, DataModelEntity } from '@datahub/data-models/constants/entity';
import { DataConstructChangeManagementEntity } from '@datahub/data-models/entity/data-construct-change-management/data-construct-change-management-entity';
import { inject as service } from '@ember/service';
import DataModelsService from '@datahub/data-models/services/data-models';
import { alias } from '@ember/object/computed';

interface IChangeLogSearchProviderArgs {
  /**
   * The entity which resides as a property on the `data-construct-change-management` entity under `OwningEntity`
   */
  entity: DataModelEntityInstance;
}

/**
 * Aspects to show in search results for change log
 */
const aspects: Array<keyof Com.Linkedin.Metadata.Aspect.DataConstructChangeManagementAspect> = [];

/**
 * A Dummy component that has no significant logic around API calls or data transformation but serves
 * as a wrapper for providing the required search configs and params.
 */
export default class ChangeLogSearchProvider extends Component<IChangeLogSearchProviderArgs> {
  /**
   * Search config being passed over to leverage existing search container
   */
  searchConfig?: IEntityRenderCommonPropsSearch = {
    attributes: [],
    showFacets: false,
    defaultAspects: aspects
  };

  /**
   * TODO: [META-11886] Investigate Pagination of results in ChangeLogTable
   * Number of search results we want to display in a single page.
   */
  pageSize = 200;

  /**
   * The search keyword being provided to dictate what aspect of the entity we search by
   * Here we escape the double quotes with a backslash to ensure encoding meets the search query requirements
   */
  keyword = encodeURIComponent(`owningEntity:\\"${this.args.entity.urn}\\"`);

  /**
   * Get the entity whose display name we care about and whose instances we are searching for
   */
  get changeManagementEntity(): DataModelEntity {
    return this.dataModels.getModel('data-construct-change-management');
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
  @alias('changeManagementEntity.displayName')
  entityDisplayName: DataModelName = DataConstructChangeManagementEntity.displayName;
}
