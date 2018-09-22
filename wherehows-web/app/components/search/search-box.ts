import Component from '@ember/component';
import { set } from '@ember/object';

export default class SearchBox extends Component {
  text: string;

  onSearch: (q: string) => void;

  onUserType: (text: string) => Promise<Array<string>>;

  blur() {
    if (this.element) {
      const input = this.element.querySelector('input');
      if (input) {
        input.blur();
      }
    }
  }

  defaultHighlighted() {
    return;
  }

  onBeforeUserType(text: string) {
    set(this, 'text', text);

    return this.onUserType(text);
  }
  /**
   * Power select on change action
   */
  onChange(selected: string) {
    set(this, 'text', selected);
    this.onSearch(selected);
    this.blur();
  }

  onSubmit() {
    this.onSearch(this.text);
    this.blur();
  }
}
