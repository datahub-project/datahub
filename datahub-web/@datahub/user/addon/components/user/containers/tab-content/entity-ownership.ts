import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../../templates/components/user/containers/tab-content/entity-ownership';
import { layout } from '@ember-decorators/component';
import { computed } from '@ember/object';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { IEntityRenderCommonPropsSearch } from '@datahub/data-models/types/search/search-entity-render-prop';
import { entityFromTabId } from '@datahub/user/utils/tabownership';
import DataModelsService from '@datahub/data-models/services/data-models';
import { inject as service } from '@ember/service';

/**
 * The UserContainersTabContentEntityOwnership component is used do display the tab content for the
 * ownership tab itself. Taking in the selected tab as a parameter, we can reason the entity for
 * which to show ownership relationship with the person entity context
 */
@layout(template)
export default class UserContainersTabContentEntityOwnership extends Component {
  /**
   * Injection of data modeling service to get the current implementation of the user's person
   * entity.
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * Given parameters from the profile page content parent component. With the selected tab, we can
   * reason about the entity for which to display ownership relationship information to our person
   * entity context
   */
  params?: { selectedTab: string };

  /**
   * Given the selected tab from the parameters, map to the entity for which to display ownership
   * relationship information
   */
  @computed('params.selectedTab')
  get ownedEntity(): DataModelEntity | void {
    const { params, dataModels } = this;

    if (params && params.selectedTab) {
      const entity = entityFromTabId(params.selectedTab, dataModels);
      return entity;
    }
  }

  /**
   * Given the selected ownedEntity, we can figure out the search result field properties
   * configured for that entity in order to show the correct results. This is because we are
   * currently leveraging search to show ownership relationships.
   */
  @computed('ownedEntity')
  get searchConfig(): IEntityRenderCommonPropsSearch | void {
    const { ownedEntity } = this;

    if (ownedEntity) {
      const { userEntityOwnership, search } = ownedEntity.renderProps;
      return userEntityOwnership || search;
    }
  }
}
