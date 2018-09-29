import Component from '@ember/component';
import { set, setProperties } from '@ember/object';
import { TaskInstance } from 'ember-concurrency';
import { IPowerSelectAPI } from 'wherehows-web/typings/modules/power-select';

type PromiseOrTask<T> = PromiseLike<T> | TaskInstance<T> | undefined;

/**
 * Will check if the type is a promise or a task. The difference is that
 * a task is cancellable where as a promise not (for now).
 * @param obj the object to check
 */
function isTask<T>(obj: PromiseOrTask<T>): obj is TaskInstance<T> {
  return typeof obj !== 'undefined' && (<TaskInstance<T>>obj).cancel !== undefined;
}

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
   * Will blur the input
   */
  blur() {
    this.close();

    if (this.element) {
      const input = this.element.querySelector('input');
      if (input) {
        input.blur();
      }
    }
  }

  /**
   * Will open the suggestion box if power select is available
   */
  open() {
    if (this.powerSelectApi) {
      this.powerSelectApi.actions.open();
    }
  }

  /**
   * Will close the suggestion box if power select is available
   */
  close() {
    if (this.powerSelectApi) {
      this.powerSelectApi.actions.close();
    }
  }

  /**
   * When the input transitioned from focus->blur
   * Reset suggestions, save text and cancel previous search.
   */
  onBlur() {
    set(this, 'text', this.inputText);
    this.cancelSearchTask();
    set(this, 'suggestions', []);
  }

  /**
   * When the input transitioned from blur->focus
   * Restore inputText value from text, open suggestions, and search latest term
   */
  onFocus(pws: IPowerSelectAPI<string>) {
    set(this, 'inputText', this.text);
    if (this.text) {
      pws.actions.search(this.text);
      pws.actions.open();
    }
  }

  /**
   * When suggestion box opens
   */
  onOpen(pws: IPowerSelectAPI<string>) {
    set(this, 'powerSelectApi', pws);
  }

  /**
   * When suggestion box closes
   */
  onClose() {
    set(this, 'powerSelectApi', undefined);
  }

  /**
   * Before we call onUserType, we cancel the last search task if available
   * and save the new one
   * @param text user typed text
   */
  onBeforeUserType(text: string) {
    this.cancelSearchTask();

    const searchTask = this.onUserType(text);
    setProperties(this, { searchTask });
    return searchTask;
  }

  /**
   * Power select forces us to return undefined so
   * highlighted is undefined.
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
      set(this, 'text', selected);
      this.onSearch(selected);
      this.blur();
    }
  }

  /**
   * When user intents to perform a search
   */
  onSubmit() {
    if (this.inputText && this.inputText.trim().length > 0) {
      this.onSearch(this.inputText);
      this.blur();
    }
  }
}
