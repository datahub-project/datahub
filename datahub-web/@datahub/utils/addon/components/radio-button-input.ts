import Component from '@glimmer/component';
import { computed, action } from '@ember/object';
import { isEqual } from '@ember/utils';
import { run } from '@ember/runloop';

export default class RadioButtonInput extends Component<{
  name: string;
  disabled?: boolean;
  value: string;
  groupValue: string;
  changed: (value: string) => void;
}> {
  @computed('args.{groupValue,value}')
  get checked(): boolean {
    return isEqual(this.args.groupValue, this.args.value);
  }

  @computed('checked')
  get checkedStr(): string | void {
    const { checked } = this;

    if (typeof checked === 'boolean') {
      return checked.toString();
    }
  }

  invokeChangedAction(): void {
    const { changed, value } = this.args;

    if (changed) {
      changed(value);
    }
  }

  @action
  change(): void {
    const { value, groupValue } = this.args;

    if (groupValue !== value) {
      run.once(this, 'invokeChangedAction');
    }
  }
}
