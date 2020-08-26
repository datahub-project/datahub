import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/nacho-table-header';
import { classNames, tagName } from '@ember-decorators/component';
import { DefaultTableComponents, DefaultTableClasses } from '../constants/default-table-properties';
import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';
import { not } from '@ember/object/computed';

@tagName('thead')
@classNames(DefaultTableClasses.header)
export default class NachoTableHeader<T> extends Component {
  layout = layout;

  /**
   * The path to the default rendered element for each cell in this header. Can be used when the user is
   * requiring blocked elements
   * @type {string}
   */
  tableTitleComponent = DefaultTableComponents.title;

  /**
   * Possibly passed in configs for the table component, should come from nacho-table and its consumer.
   * @type {INachoTableConfigs}
   */
  tableConfigs!: INachoTableConfigs<T>;

  /**
   * Shortcut calculation to determine if we actually have no configs passed in. Useful for quick reference
   * in the template file
   * @type {ComputedProperty<boolean>}
   */
  @not('tableConfigs')
  noConfigs!: boolean;
}
