import Component from '@ember/component';
import { classNames } from '@ember-decorators/component';
import { inject as service } from '@ember/service';
import Search from 'wherehows-web/services/search';
import { action, computed, get } from '@ember/object';
import { IEntityRenderCommonPropsSearch } from '@datahub/data-models/types/search/search-entity-render-prop';
import { ISearchResultMetadata } from '@datahub/data-models/types/entity/search';
import { DataModelEntityInstance } from '@datahub/data-models/addon/constants/entity';
import { KeyNamesWithValueType } from '@datahub/utils/types/base';

// keys that return string (excluding undefined key)
type CompatibleKeysThatReturnString = Exclude<KeyNamesWithValueType<DataModelEntityInstance, string>, undefined>;

/**
 * Defines the SearchResult class which renders an individual search result item and performs / invokes some
 * analytics related tasks to track content impression and interaction
 * @export
 * @class SearchResult
 * @extends {Component}
 */
@classNames('search-result')
export default class SearchResult extends Component {
  /**
   * Reference to the search service
   * @type {Search}
   * @memberof SearchResult
   */
  @service
  search: Search;

  /**
   * Metadata used in template
   */
  meta?: ISearchResultMetadata<DataModelEntityInstance>;

  /**
   * Result used in template
   */
  result!: DataModelEntityInstance;

  /**
   * Config for search for this entity
   */
  searchConfig!: IEntityRenderCommonPropsSearch;
  /**
   * Will return the name of the entity. By default it will use
   * 'name' as field to fetch the name, otherwise it should be
   * specified in 'entityNameField'
   */
  @computed('searchConfig.entityNameField', 'entity')
  get name(): string | undefined {
    const { result, searchConfig } = this;
    const { searchResultEntityFields = {} } = searchConfig;
    const { name = 'name' } = searchResultEntityFields || {};

    return get(result, name as CompatibleKeysThatReturnString);
  }

  /**
   * Will return the description of the entity. By default it will use
   * 'description' as field to fetch the description, otherwise it should be
   * specified in 'entityDescriptionField'
   */
  @computed('searchConfig.descriptionField', 'entity')
  get description(): string | undefined {
    const { result, searchConfig } = this;
    const { searchResultEntityFields = {} } = searchConfig;
    const { description = 'description' } = searchResultEntityFields || {};

    return result[description as CompatibleKeysThatReturnString];
  }

  /**
   * Will return the pictureUrl of the entity. By default it won't return any.
   * `entityPictureUrlField` needed to be specified to render this field
   */
  @computed('searchConfig.entityPictureUrlField', 'entity')
  get pictureUrl(): string | void {
    const { result, searchConfig = { searchResultEntityFields: { pictureUrl: '' } } } = this;
    const { searchResultEntityFields = {} } = searchConfig;
    const { pictureUrl = undefined } = searchResultEntityFields || {};

    if (result && pictureUrl) {
      return get(result, pictureUrl as CompatibleKeysThatReturnString);
    }
  }

  /**
   * When a search result is clicked with the intent of navigating to it inform
   * the search service.
   * This allows us to explicity indicate that a transition to a search result is in progress
   * @param {string} resultUrn urn for the search result item
   * @param {MouseEvent} _e
   * @memberof SearchResult
   */
  @action
  onResultClick(resultUrn: string, _e: MouseEvent): void {
    this.search.didClickSearchResult(resultUrn);
  }
}
