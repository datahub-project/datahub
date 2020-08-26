import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/nacho-table-row';
import { classNames, tagName } from '@ember-decorators/component';
import { DefaultTableComponents, DefaultTableClasses } from '../constants/default-table-properties';
import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';
import { not } from '@ember/object/computed';

@tagName('tr')
@classNames(DefaultTableClasses.row)
export default class NachoTableRow<T> extends Component {
  layout = layout;

  tableCellComponent = DefaultTableComponents.cell;

  /**
   * Helps us more easily render the various classnames in our template
   * @type {enum DefaultTableClasses}
   */
  tableClasses = DefaultTableClasses;

  /**
   * Passed in table configurations. This originates from the nacho-table component and its
   * consumer
   * @type {INachoTableConfigs}
   */
  tableConfigs!: INachoTableConfigs<T>;

  /**
   * Passed in, or config set, configuration that can originate from the nacho-table compoment or a
   * user set config that determines the path of the component we want to use to set a table row
   * @type {string}
   */
  tableRowComponent!: string;

  /**
   * Quick computation to determine whether or not we have any table configs, useful for templates
   */
  @not('tableConfigs')
  noConfigs!: boolean;
}
