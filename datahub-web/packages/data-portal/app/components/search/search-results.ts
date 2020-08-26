import Component from '@ember/component';
import { computed } from '@ember/object';
import { IDataModelEntitySearchResult } from '@datahub/data-models/types/entity/search';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity/index';

/**
 * Search results component for DataHub
 */
export default class SearchResults extends Component {
  /**
   * Message to display when there is no search results.
   * If empty a generic msg will be shown
   */
  noResultMsg?: string;

  /**
   * Component argument: what is the page size
   */
  pageSize: number;

  /**
   * Search results data that will be used to render the page
   */
  result: IDataModelEntitySearchResult<DataModelEntityInstance>;

  /**
   * Returns the last item number of the showing results
   */
  @computed('result.{page,count}', 'pageSize')
  get showingMax(): number {
    const { page, count } = this.result || { page: 0, count: 0 };
    const showing = this.pageSize * page;

    return showing > count ? count : showing;
  }

  /**
   * Returns the first item number of the showing results
   */
  @computed('showingMax')
  get showingMin(): number {
    const { page } = this.result || { page: 0 };
    return this.pageSize * (page - 1) + 1;
  }
}
