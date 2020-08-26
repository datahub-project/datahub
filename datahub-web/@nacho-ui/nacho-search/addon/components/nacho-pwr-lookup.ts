import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/nacho-pwr-lookup';
import { classNames } from '@ember-decorators/component';
import { action } from '@ember/object';
import { set } from '@ember/object';
import { setProperties } from '@ember/object';
import { IPowerSelectAPI } from 'nacho-search';
import { typeOf } from '@ember/utils';
import { Keyboard } from '../constants/keyboard';

/**
 * The stepMap is used for keyboard event translation where up arrows mean we are decreasing our position in
 * the list and down arrows mean we are increasing our position in the list
 * @type {Object}
 */
const stepMap: { [key: number]: number } = {
  [Keyboard['ArrowUp']]: -1,
  [Keyboard['ArrowDown']]: 1
};

/**
 * The nacho-pwr-lookup is a small, no frills component that helps with simple search tasks such as a typeahead
 * lookup inside a table, where we need something simple instead of a fullblown search experience. We wrap around
 * ember-power-select with a basic interface, but the actual act of searching / handling results should be delegated
 * to the container component, or an extended one
 * @example
 * {{nacho-pwr-lookup
 *   suggestionLimit=number
 *   searchPlaceholder=stringIsOptional
 *   searchResolver=functionOrActionInterfaceDefinedInClass
 *   confirmResult=functionOrActionInterfaceDefinedInClass
 * }}
 */
@classNames('nacho-pwr-lookup')
export default class NachoPwrLookup extends Component {
  layout = layout;

  /**
   * External parameter to determine how many results we should show per typeahead lookup
   * @type {number}
   * @default 10
   */
  suggestionLimit!: number;

  /**
   * External parameter to render a string as the placeholder for the search input
   * @type {string}
   * @default ""
   */
  searchPlaceholder!: string;

  /**
   * External action passed in as a handler required to perform the lookahead search for the power select
   * component. Function definition is based on ember-power-select requirements. Most common use case of
   * searchResolver would be in the body to async fetch results, then return asyncResultsCallback(results);
   * @example
   * searchResolver(query, scb, asyncResults) {
   *   const ldapRegex = new RegExp(`^${userNameQuery}.*`, 'i');
   *   const { userEntitiesSource = [] } = await getUserEntities();
   *   asyncResults(userEntitiesSource.filter(entity => ldapRegex.test(entity)));
   * }
   */
  searchResolver!: (
    query: string,
    syncResultsCallback: (results: Array<string>) => void,
    asyncResultsCallback: (results: Array<string>) => void
  ) => Promise<void>;

  /**
   * External action passed in as a handler required to confirm the result upon user action.
   * @example
   * confirmResult(userName) {
   *   if (!userName) return;
   *   const findUser = parentComponent.findUser;
   *   const userEntity = await findUser(userName);
   *   if (userEntity) ... do something;
   * }
   */
  confirmResult!: (result: string) => Promise<void>;

  /**
   * The currently selected entity from the dropdown by the user
   * @type {string}
   */
  selectedEntity!: string;

  /**
   * In a separate vein than the actual typeahead, this property helps us create an "autocomplete"
   * suggestion for the user based on their input
   * @type {string}
   */
  suggestedText!: string;

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
      showPlaceholder: !this.selectedEntity
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

  constructor() {
    // eslint-disable-next-line prefer-rest-params
    super(...arguments);

    // Setting necessary default values for component
    typeof this.searchResolver === 'function' || (this.searchResolver = (): Promise<void> => Promise.resolve());
    typeof this.confirmResult === 'function' || (this.confirmResult = (): Promise<void> => Promise.resolve());
    typeof this.suggestionLimit === 'number' || (this.suggestionLimit = 10);
    typeof this.searchPlaceholder === 'string' || (this.searchPlaceholder = '');
  }

  /**
   * On change of user input, conducts a search to find the corresponding typeahead suggestions list
   * @param keyword - keyword entered by user passed from the power-select component
   */
  @action
  onSearch(keyword: string): void {
    // Note: Property defined on base user-lookup component
    this.searchResolver(
      keyword,
      () => {},
      asyncResults => {
        if (typeOf(asyncResults) === 'array') {
          const newSuggestions = asyncResults.slice(0, this.suggestionLimit);
          setProperties(this, {
            suggestionOptions: newSuggestions,
            suggestedText: newSuggestions[0]
          });
        }
      }
    );
  }

  /**
   * Action that gets called from user keystroke event. The side effects of this action take place before
   * the event is propogated to and handled by the power select component itself. This calculates whether
   * the user has entered an arrow up or arrow down key and if so, changes the autocomplete text to the
   * next value that will be highlighted based on the list position
   * @param params - api object provided by the ember power select component
   * @param keyboardEvent - event object created by the keypress
   */
  @action
  onKeyPress(params: IPowerSelectAPI<string>, keyboardEvent: KeyboardEvent): boolean {
    const keyCode = keyboardEvent.keyCode;
    // Helps determine the "next index" to check in the list of suggestions to attempt to highlight
    const step = stepMap[keyCode] || 0;
    const { highlighted, results, options } = params;

    // Figure out where highlighted is in the options or results
    if (highlighted && step) {
      const stepList = options || results;
      const currentHighlightedIdx = stepList.indexOf(highlighted);

      let nextIdx = currentHighlightedIdx + step;
      // Make sure the attempted "next" is still within the limits of the list
      if (nextIdx < 0 || nextIdx >= stepList.length) {
        nextIdx -= step;
      }
      // This modifies the secondary "autocomplete" to match with our next highlighted suggestion
      set(this, 'suggestedText', stepList[nextIdx]);
    }
    // Prevents the tab key from skipping to next element and also autocompletes our text
    if (keyCode === Keyboard['Tab']) {
      keyboardEvent.preventDefault();
      keyboardEvent.stopPropagation();
      set(this, 'selectedEntity', this.suggestedText);
      return false;
    }
    // Treats using the enter key the same as if the user had triggered a selection event
    if (keyCode === Keyboard['Enter']) {
      this.actions.onChangeSelection.call(this, this.selectedEntity);
      return false;
    }

    return true;
  }

  /**
   * Triggered when the user presses enter in the typeahead or clicks on a name in the typeahead suggestion
   * list, will trigger the external handler to do something with the chosen result
   * @param selection - selected option in the dropdown list
   */
  @action
  onChangeSelection(selection: string): void {
    if (typeOf(selection) === 'string') {
      setProperties(this, {
        selectedEntity: '',
        suggestedText: ''
      });

      this.confirmResult(selection);
    }
  }
}
