import Component from '@ember/component';
import { set, setProperties } from '@ember/object';
import { IPowerSelectAPI } from 'wherehows-web/typings/modules/power-select';
import { isTask } from 'wherehows-web/utils/helpers/functions';
import { PromiseOrTask } from 'wherehows-web/typings/generic';

/**
 * Presentation component that renders a search box
 */

export default class SearchBox extends Component {
  /**
   * HBS Expected Parameter
   * The value of the input
   * note: recommend (readonly) helper.
   */
  text!: string;

  /**
   * HBS Expected Parameter
   * Action when the user actually wants to search
   */
  onSearch!: (q: string) => void;

  /**
   * HBS Expected Parameter
   * Action when the user types into the input, so we can show suggestions
   */
  onUserType!: (text: string) => PromiseOrTask<Array<string>>;

  /**
   * internal field to save temporal inputs in the text input
   */
  inputText: string;

  /**
   * suggestions array that we will use to empty the current suggestions
   */
  suggestions: Array<string>;

  /**
   * when suggestions box is open, we will save a reference to power-select
   * se we can close it.
   */
  powerSelectApi?: IPowerSelectAPI<string>;

  /**
   * When a search task is on going, we save it so we can cancel it when a new one comes.
   */
  searchTask?: PromiseOrTask<Array<string>>;

  /**
   * When new attrs, update inputText with latest text
   */
  didReceiveAttrs() {
    set(this, 'inputText', this.text);
  }

  /**
   * Will cancel searchTask if available
   */
  cancelSearchTask() {
    if (isTask(this.searchTask)) {
      this.searchTask.cancel();
    }
    set(this, 'searchTask', undefined);
  }

  /**
   * When the input transitioned from focus->blur
   * Reset suggestions, save text and cancel previous search.
   */
  onBlur() {
    this.cancelSearchTask();
    set(this, 'text', this.inputText);
  }

  /**
   * When the input transitioned from blur->focus
   * Restore inputText value from text, open suggestions, and search latest term
   */
  onFocus(pws: IPowerSelectAPI<string>) {
    setProperties(this, {
      inputText: this.text,
      powerSelectApi: pws
    });
    if (this.text) {
      pws.actions.search(this.text);
      pws.actions.open();
    }
  }

  /**
   * Before we call onUserType, we cancel the last search task if available
   * and save the new one
   * @param text user typed text
   */
  onBeforeUserType(text: string) {
    this.cancelSearchTask();

    const searchTask = this.onUserType(text);
    set(this, 'searchTask', searchTask);
    return searchTask;
  }

  /**
   * Power select forces us to return undefined to prevent to select
   * the first item on the list.
   */
  defaultHighlighted() {
    return;
  }

  /**
   * When user types text we save it
   * @param text user typed text
   */
  onInput(text: string) {
    set(this, 'inputText', text);
  }

  /**
   * When user selects an item from the list
   */
  onChange(selected: string) {
    if (selected && selected.trim().length > 0) {
      setProperties(this, {
        text: selected,
        inputText: selected
      });
      this.onSearch(selected);
    }
  }

  /**
   * When user intents to perform a search
   */
  onSubmit() {
    if (this.inputText && this.inputText.trim().length > 0) {
      // this will prevent search text from jitter
      set(this, 'text', this.inputText);
      this.onSearch(this.inputText);
      if (this.powerSelectApi) {
        this.powerSelectApi.actions.close();
      }
    }
  }
}
