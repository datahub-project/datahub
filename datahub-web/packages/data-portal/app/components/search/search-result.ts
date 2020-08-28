import Component from '@ember/component';
import { classNames } from '@ember-decorators/component';
import { inject as service } from '@ember/service';
import Search from '@datahub/shared/services/search';
import { action, computed } from '@ember/object';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { ISearchResultMetadata } from '@datahub/data-models/types/entity/search';
import { IEntityRenderCommonPropsSearch } from '@datahub/data-models/types/search/search-entity-render-prop';
import { KeyNamesWithValueType } from '@datahub/utils/types/base';
import { assertComponentPropertyNotUndefined } from '@datahub/utils/decorators/assert';

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
  @assertComponentPropertyNotUndefined
  search: Search;

  /**
   * Metadata used in template
   */
  meta?: ISearchResultMetadata<DataModelEntityInstance>;

  /**
   * Result used in template
   */
  @assertComponentPropertyNotUndefined
  result: DataModelEntityInstance;

  /**
   * Config for search for this entity
   */
  @assertComponentPropertyNotUndefined
  searchConfig: IEntityRenderCommonPropsSearch;

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

    return result[name as CompatibleKeysThatReturnString];
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
  get pictureUrl(): string | undefined | void {
    const { result, searchConfig } = this;
    const { searchResultEntityFields = {} } = searchConfig;
    const { pictureUrl = undefined } = searchResultEntityFields || {};
    if (pictureUrl) {
      return result[pictureUrl as CompatibleKeysThatReturnString];
    }
  }

  /**
   * When a search result is clicked with the intent of navigating to it inform
   * the search service.
   * This allows us to explicity indicate that a transition to a search result is in progress
   * @param {string} resultUrn urn for the search result item
   * @param {number} absolutePosition The absolute position of the search result in the total list of search results for the search query, independent of pagination. This is null when the impressed content is not a result item, but, for example, a search result facet
   * @memberof SearchResult
   */
  @action
  onResultClick(resultUrn: string, absolutePosition: number): void {
    this.search.didClickSearchResult(resultUrn, absolutePosition);
  }
}
