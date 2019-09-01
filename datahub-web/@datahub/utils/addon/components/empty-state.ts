import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/empty-state';
import { layout, classNames } from '@ember-decorators/component';

@layout(template)
@classNames('empty-state')
export default class EmptyState extends Component {
  /**
   * Default heading for the empty state component
   * @type {string}
   */
  heading: string = 'No data found';

  /**
   * Sub heading text rendered beneath the empty state call to action if present
   */
  subHead?: string;
}
