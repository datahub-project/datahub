import Component from '@ember/component';
import { get, set, setProperties, action } from '@ember/object';
import { typeOf } from '@ember/utils';
import { suggestionLimit } from 'datahub-web/constants/typeahead';
import { IPowerSelectAPI } from '@nacho-ui/search/types/nacho-search';
import { noop } from 'lodash';
import { fetchFacetValue } from 'datahub-web/utils/parsers/helpers';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { classNames } from '@ember-decorators/component';
import { IOwner } from 'datahub-web/typings/api/datasets/owners';
import { inject as service } from '@ember/service';
import DataModelsService from '@datahub/data-models/services/data-models';
import { defaultOwnerProps } from 'datahub-web/constants/datasets/owner';

@classNames('user-lookup')
export default class PowerUserLookup extends Component {
  /**
   * Injection of the application's data modeling service
   */
  @service('data-models')
  dataModels!: DataModelsService;

  /**
   * The currently selected person from the dropdown by the user
   */
  selectedEntity: string;

  /**
   * In a separate vein than the actual typeahead, this property helps us create an "autocomplete"
   * suggestion for the user based on their input
   * @type {string}
   */
  suggestedText: string;

  /**
   * Suggestion options to be rendered in the dropdown for the typeahead
   * @type {Array<string>}
   */
  suggestionOptions: Array<string> = [];

  /**
   * Whether or not we want to show the placeholder text in the typeahead box. When the user focuses in,
   * we want to remove both the placeholder text and the "+" icon in the box
   * @type {boolean}
   */
  showPlaceholder = true;

  /**
   * When the user focuses in on our component, we want to remove the placeholder text and the "+" icon in
   * the typeahead box.
   */
  focusIn(): void {
    set(this, 'showPlaceholder', false);
  }

  /**
   * This hook is used to ensure overall component behavior is consistent with power-select behavior. Since
   * the power select typeahead is cleared when input loses focus, we use this to clear our autosuggest
   */
  focusOut(): void {
    setProperties(this, {
      suggestedText: '',
      showPlaceholder: !get(this, 'selectedEntity')
    });
  }

  /**
   * Overwrites the basic highlight function inside the ember-power-select component (which the typeahead
   * is a wrapper around) and allows us to ensure that the first option is selected everytime a new search
   * was triggered by user input
   * @param params - api object provided by ember-power-select component into this closure method
   */
  highlight(params: IPowerSelectAPI<string>): string | Array<string> {
    const { results, highlighted, selected, options } = params;
    const selectionList = options || results;
    // If we have a list of options, return the first item in that list to highlight. Otherwise, fallback to
    // the already highlighted item (if any) or finally the item the typehead currently thinks is selected.
    return typeOf(selectionList) === 'array' ? selectionList[0] : highlighted || selected;
  }

  /**
   * On change of user input, conducts a search to find the corresponding typeahead suggestions list
   * @param keyword - keyword entered by user passed from the power-select component
   */
  @action
  async onSearch(keyword: string): Promise<void> {
    const newSuggestions = (await fetchFacetValue('ldap', keyword, PersonEntity)).slice(0, suggestionLimit);
    setProperties(this, {
      suggestionOptions: newSuggestions,
      suggestedText: newSuggestions[0]
    });
  }

  /**
   * Triggered when the user presses enter in the typeahead or clicks on a name in the typeahead suggestion
   * list, will trigger a search for the specified person and call the didFindUser passed in handler for them
   * @param selection - selected option in the dropdown list
   */
  @action
  async onChangeSelection(selection: string): Promise<void> {
    if (typeOf(selection) === 'string') {
      setProperties(this, {
        selectedEntity: '',
        suggestedText: ''
      });
    }

    const { name } = (await this.dataModels.createInstance(
      PersonEntity.displayName,
      PersonEntity.urnFromUsername(selection)
    )) as PersonEntity;
    const owner: IOwner = {
      ...defaultOwnerProps,
      userName: selection,
      name
    };

    this.didFindUser(owner);
  }

  /**
   * External action triggered when a user is selected from type ahead
   * @param {IOwner} user the owner instance found matching the sought user
   * @memberof UserLookup
   */
  didFindUser: (user: IOwner) => void = noop;
}
