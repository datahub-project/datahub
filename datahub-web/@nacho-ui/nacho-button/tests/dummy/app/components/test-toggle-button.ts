import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/test-toggle-button';
import { layout, classNames } from '@ember-decorators/component';
import { action } from '@ember/object';
import { set } from '@ember/object';

@layout(template)
@classNames('test-container')
export default class TestToggleButton extends Component {
  value = 'Pikachu';

  leftValue = 'Pikachu';
  rightValue = 'Eevee';

  @action
  onChangeValue(newValue: string): void {
    set(this, 'value', newValue);
  }
}
