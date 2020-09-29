import Component from '@glimmer/component';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { ISearchDataWithMetadata, IDataModelEntitySearchResult } from '@datahub/data-models/types/entity/search';
import { action } from '@ember/object';
import { tracked } from '@glimmer/tracking';
import { RecipientType } from '@datahub/shared/constants/change-management';
import { debounce } from '@ember/runloop';

/**
 * Interface for recipient power select's arguments
 */
interface IRecipientPowerSelectArgs {
  // Search result from user query
  result: IDataModelEntitySearchResult<ISearchDataWithMetadata<PersonEntity>>;
  // placeholder method that is called upon when user enters a query
  onSearch: (query: string) => Promise<void>;
  // List of individual recipients to be displayed
  individualRecipients: Array<string>;
  // placeholder method that is called when user wants to add a new recipient
  addRecipient: (recipientType: string, recipientName: string) => void;
  // placeholder method that is called when user wants to delete a recipient
  removeRecipient: (recipientType: string, recipientName: string, index: number) => void;
}

/**
 * Properties that exist on the options that an user sees in the power select dropdown
 */
interface IRecipientOption {
  fullName: string;
  email: string;
  username: string;
}

/**
 * Styling class for the component
 */
const baseClass = 'recipient-power-select';

/**
 *
 */
export default class RecipientPowerSelect extends Component<IRecipientPowerSelectArgs> {
  /**
   * Attached to component for easier access from template.
   */
  baseClass = baseClass;

  /**
   * Types of recipients possible
   */
  recipientType: Record<string, string> = {
    individualRecipient: RecipientType.IndividualRecipient,
    distributionList: RecipientType.DistributionList
  };

  /**
   * Flag that controls if we want to display the power select dropdown or not
   */
  @tracked
  showOptions = false;

  /**
   * A getter that massages the `result` data from the search api into something meaningful and light that can be
   * used by power-select as options.
   */
  get options(): Array<IRecipientOption> {
    const { result } = this.args;

    const dataArray = result?.data || [];
    const dataEntities: Array<PersonEntity> = dataArray.mapBy('data');

    // Fetch the usernames
    const userNames: Array<string> = dataEntities.mapBy('entity').map(resultResponse => resultResponse.username);
    // Fetch the name , email and append username to it.

    const options = dataEntities
      .mapBy('entity')
      .map(resultResponse => resultResponse.info)
      .map((person, index) => {
        return { fullName: person.fullName, email: person.email, username: userNames[index] };
      });
    return options;
  }

  /**
   * Triggers a search when user enters 2 letters or more in the input field.
   *
   * @param keyEvent HTML event from a keypress
   */
  @action
  onSearchLocal(keyEvent: KeyboardEvent): Promise<void> | void {
    const userInputField = keyEvent.target as HTMLInputElement;
    if (userInputField && userInputField.value.length >= 2 && typeof this.args.onSearch === 'function') {
      const keyword = userInputField.value;
      this.showOptions = true;
      return this.onSearchDebounced(keyword);
    } else {
      this.showOptions = false;
    }
  }

  onSearchDebounced(keyword: string): void {
    debounce(this, this.args.onSearch, keyword, 250);
  }

  /**
   * Incharge of adding a recipient when the user selects an option from the list of user suggestions.a1
   *
   * @param selection One of the options in the power-select dropdown
   */
  @action
  onChangeSelection(selection: IRecipientOption): void {
    this.args.addRecipient(this.recipientType.individualRecipient, selection.username);
    this.showOptions = false;
  }
}
