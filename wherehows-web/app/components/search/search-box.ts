import Component from '@ember/component';
import { set, setProperties } from '@ember/object';
import { IPowerSelectAPI } from 'wherehows-web/typings/modules/power-select';
import { PromiseOrTask, isTask } from 'wherehows-web/utils/helpers/ember-concurrency';

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
  onTypeahead!: (text: string) => PromiseOrTask<Array<string>>;

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
  typeaheadTask?: PromiseOrTask<Array<string>>;

  /**
   * When new attrs, update inputText with latest text
   */
  didReceiveAttrs(): void {
    set(this, 'inputText', this.text);
  }

  /**
   * Will cancel typeaheadTask if available
   */
  cancelTypeaheadTask(): void {
    if (isTask(this.typeaheadTask)) {
      this.typeaheadTask.cancel();
    }
    set(this, 'typeaheadTask', undefined);
  }

  /**
   * When the input transitioned from focus->blur
   * Reset suggestions, save text and cancel previous search.
   */
  onBlur(): void {
    this.cancelTypeaheadTask();
    set(this, 'text', this.inputText);
  }

  /**
   * When the input transitioned from blur->focus
   * Restore inputText value from text, open suggestions, and search latest term
   */
  onFocus(pws: IPowerSelectAPI<string>): void {
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
   * Before we call onTypeahead, we cancel the last search task if available
   * and save the new one
   * @param text user typed text
   */
  typeahead(text: string): PromiseOrTask<Array<string>> {
    this.cancelTypeaheadTask();

    const typeaheadTask = this.onTypeahead(text);
    set(this, 'typeaheadTask', typeaheadTask);
    return typeaheadTask;
  }

  /**
   * Power select forces us to return undefined to prevent to select
   * the first item on the list.
   */
  defaultHighlighted(): undefined {
    return;
  }

  /**
   * When user types text we save it
   * @param text user typed text
   */
  onInput(text: string): void {
    set(this, 'inputText', text);
  }

  /**
   * When user selects an item from the list
   */
  onChange(selected: string): void {
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
  onSubmit(): void {
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
