import Component from '@ember/component';
import { set, setProperties } from '@ember/object';
import { TaskInstance } from 'ember-concurrency';
import { IPowerSelectAPI } from 'wherehows-web/typings/modules/power-select';

type PromiseOrTask<T> = PromiseLike<T> | TaskInstance<T> | undefined;

function isTask<T>(obj: PromiseOrTask<T>): obj is TaskInstance<T> {
  return typeof obj !== 'undefined' && (<TaskInstance<T>>obj).cancel !== undefined;
}

export default class SearchBox extends Component {
  text!: string;
  onSearch!: (q: string) => void;
  onUserType!: (text: string) => PromiseOrTask<Array<string>>;

  inputText: string;
  suggestions: Array<string>;
  powerSelectApi?: IPowerSelectAPI<string>;
  searchTask?: PromiseOrTask<Array<string>>;

  didReceiveAttrs() {
    set(this, 'inputText', this.text);
  }

  cancelSearchTask() {
    if (isTask(this.searchTask)) {
      this.searchTask.cancel();
    }
    set(this, 'searchTask', undefined);
  }

  blur() {
    this.close();

    if (this.element) {
      const input = this.element.querySelector('input');
      if (input) {
        input.blur();
      }
    }
  }

  open() {
    if (this.powerSelectApi) {
      this.powerSelectApi.actions.open();
    }
  }

  close() {
    if (this.powerSelectApi) {
      this.powerSelectApi.actions.close();
    }
  }

  onBlur() {
    set(this, 'text', this.inputText);
    this.cancelSearchTask();
    set(this, 'suggestions', []);
  }

  onFocus(pws: IPowerSelectAPI<string>) {
    set(this, 'inputText', this.text);
    if (this.text) {
      pws.actions.search(this.text);
      pws.actions.open();
    }
  }

  onOpen(pws: IPowerSelectAPI<string>) {
    set(this, 'powerSelectApi', pws);
  }

  onClose() {
    set(this, 'powerSelectApi', undefined);
  }

  onBeforeUserType(text: string) {
    this.cancelSearchTask();

    const searchTask = this.onUserType(text);
    setProperties(this, { searchTask });
    return searchTask;
  }

  defaultHighlighted() {
    return undefined;
  }

  onInput(text: string) {
    set(this, 'inputText', text);
  }

  /**
   * Power select on change action
   */
  onChange(selected: string) {
    if (selected && selected.trim().length > 0) {
      set(this, 'text', selected);
      this.onSearch(selected);
      this.blur();
    }
  }

  onSubmit() {
    if (this.inputText && this.inputText.trim().length > 0) {
      this.onSearch(this.inputText);
      this.blur();
    }
  }
}
