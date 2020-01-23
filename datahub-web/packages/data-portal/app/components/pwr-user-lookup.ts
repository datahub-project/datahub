import { get, set, setProperties } from '@ember/object';
import { typeOf } from '@ember/utils';
import { action } from '@ember/object';
import UserLookup from 'wherehows-web/components/user-lookup';
import { Keyboard } from 'wherehows-web/constants/keyboard';
import { suggestionLimit } from 'wherehows-web/constants/typeahead';
import { IPowerSelectAPI } from 'wherehows-web/typings/modules/power-select';
import { noop } from 'wherehows-web/utils/helpers/functions';

/**
 * The stepMap is used for keyboard event translation where up arrows mean we are decreasing our position in
 * the list and down arrows mean we are increasing our position in the list
 * @type {Object}
 */
const stepMap: { [key: number]: number } = {
  [Keyboard['ArrowUp']]: -1,
  [Keyboard['ArrowDown']]: 1
};

export default class PowerUserLookup extends UserLookup {
  didInsertElement() {
    super.didInsertElement();
    this.classNames = (this.classNames || []).concat('pwr-user-lookup');
    this.classNameBindings = (this.classNameBindings || []).concat('showPlaceholder:pwr-user-lookup--focused');
  }

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
  focusIn() {
    set(this, 'showPlaceholder', false);
  }

  /**
   * This hook is used to ensure overall component behavior is consistent with power-select behavior. Since
   * the power select typeahead is cleared when input loses focus, we use this to clear our autosuggest
   */
  focusOut() {
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
  highlight(params: IPowerSelectAPI<string>) {
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
  onSearch(keyword: string) {
    // Note: Property defined on base user-lookup component
    const userNamesResolver = get(this, 'userNamesResolver');
    // The async results retrieved from the resolver is what's used to populate our refreshed suggestions
    userNamesResolver(keyword, noop, asyncResults => {
      if (typeOf(asyncResults) === 'array') {
        const newSuggestions = asyncResults.slice(0, suggestionLimit);
        setProperties(this, {
          suggestionOptions: newSuggestions,
          suggestedText: newSuggestions[0]
        });
      }
    });
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
  onKeyPress(params: IPowerSelectAPI<string>, keyboardEvent: KeyboardEvent) {
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
      set(this, 'selectedEntity', get(this, 'suggestedText'));
      return false;
    }
    // Treats using the enter key the same as if the user had triggered a selection event
    if (keyCode === Keyboard['Enter']) {
      this.actions.onChangeSelection.call(this, get(this, 'selectedEntity'));
      return false;
    }

    return true;
  }

  /**
   * Triggered when the user presses enter in the typeahead or clicks on a name in the typeahead suggestion
   * list, will trigger a search for the specified person and call the didFindUser passed in handler for them
   * @param selection - selected option in the dropdown list
   */
  @action
  onChangeSelection(selection: string) {
    if (typeOf(selection) === 'string') {
      setProperties(this, {
        selectedEntity: '',
        suggestedText: ''
      });
      // Note: findUser is on the base UserLookup class
      this.actions.findUser.call(this, selection);
    }
  }
}
