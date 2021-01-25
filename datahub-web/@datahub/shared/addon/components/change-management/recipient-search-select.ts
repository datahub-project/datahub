import Component from '@glimmer/component';
import { DataModelEntityInstance, DataModelEntity } from '@datahub/data-models/constants/entity';
import { inject as service } from '@ember/service';
import DataModelsService from '@datahub/data-models/services/data-models';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { tracked } from '@glimmer/tracking';
import { action } from '@ember/object';
import { RecipientType } from '@datahub/shared/constants/change-management';
import { getResultsForEntity } from '@datahub/shared/utils/search/search-results';
import { IDataModelEntitySearchResult } from '@datahub/data-models/types/entity/search';
import { task, timeout } from 'ember-concurrency';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { GroupEntity } from '@datahub/data-models/entity/group/group-entity';

/**
 * Interface for the list of the arguments being fed into the recipient-search-select component
 */
interface IRecipientSearchSelectArgs {
  /**
   * Placeholder method that passes the action of adding a recipient back to the parent
   */
  addRecipient: (recipientType: string, recipientName: string) => void;
  /**
   * Placeholder method that passes the action of removing a recipient back to the parent
   */
  removeRecipient: (recipientType: string, index: number) => void;

  /**
   * Optional Array of ldaps of individual recipients that are fed in from the parent
   */
  individualRecipients?: Array<string>;

  /**
   * Optional Array of distributed lists / CorpGroups that are fed in from the parent
   */
  distributionLists?: Array<string>;
}

/**
 * Styling class for the component
 */
const baseClass = 'recipient-search-select';

/**
 * Time delay that the function is being debounced for
 */
const DEBOUNCE_TIME_IN_MS = 250;

/**
 * The minimum amount of characters for the user to type so that we trigger off the search and populate the recipient results
 */
const SEARCH_INPUT_MIN_LENGTH = 2;

/**
 * A component that provides an interface to allow users to search for either Individuals or Groups and add them to a collective list.
 * It abstracts the searching aspect of it like providing configs , de-bouncing the input , handing the keyword.
 *
 * It is currently able to search for either Person entity / Group entity based on how the parent chooses to provide optional arguments.
 */
export default class RecipientSearchSelect extends Component<IRecipientSearchSelectArgs> {
  /**
   * Styling class name
   */
  baseClass = baseClass;

  /**
   * Debounce time provided
   */
  debounceTimeInMs = DEBOUNCE_TIME_IN_MS;

  /**
   * Min input length for search to trigger
   */
  searchInputMinLength = SEARCH_INPUT_MIN_LENGTH;

  /**
   * The search keyword being provided to dictate what aspect of the entity we search by
   * Here we escape the double quotes with a backslash to ensure encoding meets the search query requirements
   */
  @tracked
  keyword = '';

  /**
   * Number of search results we want to display in a single page.
   */
  pageSize = 5;

  /**
   * Returns the type of entity that is being searched.
   * Defaults to PersonEntity
   */
  get searchEntityType(): DataModelEntity {
    if (this.args.distributionLists) {
      return GroupEntity;
    } else return PersonEntity;
  }

  /**
   * Returns the type of recipient that is being added / removed from the UI.
   * This value is used to tell the parent what kind of recipient we are dealing with.
   */
  get recipientType(): RecipientType {
    if (this.searchEntityType === GroupEntity) {
      return RecipientType.DistributionList;
    } else return RecipientType.IndividualRecipient;
  }
  /**
   * Injection of the data models service to access the general dataConstructChangeManagement entity class
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * Passover method to an external method incharge of adding a new recipient.
   *
   * @param recipient Person getting added to the inidvidualRecipients list
   */
  @action
  onAddRecipient(recipient: PersonEntity | GroupEntity): void {
    if (recipient instanceof PersonEntity) {
      this.args.addRecipient(this.recipientType, recipient.username);
    }
    if (recipient instanceof GroupEntity && recipient.entity?.name) {
      this.args.addRecipient(this.recipientType, recipient.entity.name);
    }
  }

  /**
   * Task responsible for fetching PersonEntity results from the search api for the user provided keyword from the Typeahead component
   */
  @task(function*(
    this: RecipientSearchSelect,
    keyword: string
  ): IterableIterator<Promise<IDataModelEntitySearchResult<DataModelEntityInstance> | undefined | void>> {
    yield timeout(this.debounceTimeInMs);
    // If we are searching for a person, we want to ensure they are active in the directory
    keyword = this.searchEntityType === PersonEntity ? `${keyword} AND active:true` : keyword;
    const results = ((yield getResultsForEntity(
      {
        facetsApiParams: {},
        keyword,
        page: 1,
        pageSize: this.pageSize
      },
      this.searchEntityType.displayName,
      this.dataModels
    )) as unknown) as IDataModelEntitySearchResult<DataModelEntityInstance> | undefined;
    return results?.data;
  })
  onSearchTask!: ETaskPromise<Array<DataModelEntityInstance> | undefined | void, string>;
}
