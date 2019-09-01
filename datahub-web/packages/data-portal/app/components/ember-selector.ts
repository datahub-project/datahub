import Component from '@ember/component';
import { classNames } from '@ember-decorators/component';
import { computed, action } from '@ember/object';
import { noop } from 'wherehows-web/utils/helpers/functions';

interface IOption<T> {
  value: T;
  label?: string;
  isDisabled?: boolean;
  isSelected?: boolean;
}

// TODO META-7964 replace Ember Selector with nacho component
@classNames('nacho-select')
export default class EmberSelector extends Component {
  selected: any;

  values: Array<any> = [];

  selectionDidChange: (selected: any) => void = noop;

  /**
   * Parse and transform the values list into a list of objects with the currently
   * selected option flagged as `isSelected`
   */

  @computed('selected', 'values')
  get content(): Array<IOption<any>> {
    const { selected = null, values = [] } = this;

    const content = values.map(option => {
      if (typeof option === 'object' && typeof option.value !== 'undefined') {
        const isSelected = option.value === selected;
        return { value: option.value, label: option.label, isSelected, isDisabled: option.isDisabled || false };
      }

      return { value: option, isSelected: option === selected };
    });

    return content;
  }

  // Reflect UI changes in the component and bubble the `selectionDidChange` action
  @action
  change(e: Event): void {
    const select = this.element.querySelector('select');
    if (e && select && e.target === select) {
      const { selectedIndex = 0 } = select;
      const { values } = this;
      const _selected = values[selectedIndex];
      const selected = typeof _selected.value !== 'undefined' ? _selected.value : _selected;

      this.set('selected', selected);

      this.selectionDidChange(_selected);
    }
  }
}
