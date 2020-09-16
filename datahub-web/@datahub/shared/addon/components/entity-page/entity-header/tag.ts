import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/entity-page/entity-header/tag';
import { layout, tagName } from '@ember-decorators/component';
import { computed } from '@ember/object';

@layout(template)
@tagName('')
export default class EntityHeaderTag extends Component {
  baseClass = 'entity-pill';

  /**
   * optional. Tooltip message if needed
   */
  tooltip?: string;

  /**
   * value can have the string which to show or a boolean to say whether we should show this tag
   * in this case, options.text must be defined
   */
  value?: string | boolean;

  /**
   * returns the text to display for this tag.
   * text can come from the value or an option in options
   */
  @computed('value')
  get text(): string | undefined {
    const { value, options } = this;
    if (typeof this.value === 'boolean' && value && options && options.text) {
      return options.text;
    }
    if (typeof value === 'string' && value.length > 0) {
      return value;
    }

    return undefined;
  }

  /**
   * Options that this component accept
   */
  options?: { text: string; state: string };
}
