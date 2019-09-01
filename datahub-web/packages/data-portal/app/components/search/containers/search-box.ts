import Component from '@ember/component';
import { inject as service } from '@ember/service';
import SearchService from 'wherehows-web/services/search';
import { alias } from '@ember/object/computed';
import { grammarProcessingSteps, typeaheadQueryProcessor } from 'wherehows-web/utils/parsers/autocomplete';
import { ISuggestionGroup } from 'wherehows-web/utils/parsers/autocomplete/types';
import { tagName } from '@ember-decorators/component';
import { computed } from '@ember/object';
import { DataModelEntity, DataModelName } from '@datahub/data-models/constants/entity';
import {
  getFacetDefaultValueForEntity,
  transformDefaultsIntoSelections
} from '@datahub/data-models/entity/utils/facets';
import { facetToParamUrl } from 'wherehows-web/utils/api/search/search';
import RouterService from '@ember/routing/router-service';
import { task, Task } from 'ember-concurrency';

/**
 * Search box container that handle all the data part for
 * the search box
 */
@tagName('')
export default class SearchBoxContainer extends Component {
  @service
  router: RouterService;

  @service
  search: SearchService;

  @alias('search.entity')
  entity?: DataModelName;

  /**
   * Search route will update the service with current keywords
   * since url will contain keyword
   */
  @alias('search.keyword')
  keyword: string;

  /**
   * Entity definition for current entity
   */
  @computed('entity')
  get dataModelEntity(): DataModelEntity | undefined {
    const { entity } = this;
    if (entity) {
      return DataModelEntity[entity];
    }
    return;
  }

  /**
   * Invokes an async function to process the typeahead query and related parameters
   * @param {string} [query=''] user input for to query typeahead endpoint with
   * @param {Entity} [entity]
   * @returns {(IterableIterator<Promise<Array<ISuggestionGroup>>>)}
   * @memberof SearchBoxContainer
   */
  @task(function*(
    query: string = '',
    entity?: DataModelEntity['displayName']
  ): IterableIterator<Promise<Array<ISuggestionGroup>>> {
    return yield typeaheadQueryProcessor(query, entity, grammarProcessingSteps);
  })
  onSearchInputTask: Task<
    Promise<Array<ISuggestionGroup>>,
    (query?: string, entity?: DataModelEntity['displayName']) => Promise<Array<ISuggestionGroup>>
  >;

  /**
   * When search actually happens, then we transition to a new route.
   * @param {string} [text=''] user input as search term
   * @param {Entity} [entity]
   * @memberof SearchBoxContainer
   */
  onSearch(text: string = '', entity: DataModelEntity['displayName'] = 'datasets'): void {
    // entity (dropdown value) might be different than this.entity (Page that you are in)
    const dataModelEntity = DataModelEntity[entity];
    const { attributes } = dataModelEntity.renderProps.search;
    const defaultFacets = facetToParamUrl(transformDefaultsIntoSelections(getFacetDefaultValueForEntity(attributes)));

    this.router.transitionTo('search', {
      queryParams: {
        entity,
        keyword: text,
        page: 1,
        facets: defaultFacets
      }
    });
  }
}
