import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/empty-state';
import { layout, classNames, classNameBindings } from '@ember-decorators/component';
import { expandOptionsAttribute } from '@datahub/utils/decorators/expand-options-attribute';

const baseClass = 'empty-state';

@layout(template)
@classNames(baseClass)
@classNameBindings(`isSuccessState:${baseClass}--success`)
@expandOptionsAttribute()
export default class EmptyState extends Component {
  /**
   * Default heading for the empty state component
   * @type {string}
   */
  heading = 'No data found';

  /**
   * Sub heading text rendered beneath the empty state call to action if present
   */
  subHead?: string;

  /**
   * Flag to indicate that content is empty because all needed tasks may have been completed by the user
   * rather than "just" missing content
   */
  isSuccessState = false;
}
