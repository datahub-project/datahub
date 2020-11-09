import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/nacho-table/nacho-table-body';
import { classNames, tagName, layout } from '@ember-decorators/component';
import {
  DefaultTableClasses,
  DefaultTableComponents
} from '@nacho-ui/core/constants/nacho-table/default-table-properties';
import { INachoTableConfigs } from '@nacho-ui/core/types/nacho-table';
import { not } from '@ember/object/computed';

@layout(template)
@tagName('tbody')
@classNames(DefaultTableClasses.body)
export default class NachoTableBody<T> extends Component {
  /**
   * For easier access in the template
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

  constructor() {
    // eslint-disable-next-line prefer-rest-params
    super(...arguments);

    const tableConfigs = this.tableConfigs;

    // Determines the proper compoment with priority of: passed in component > component given by config >
    // default component
    this.tableRowComponent =
      (typeof this.tableRowComponent === 'string' && this.tableRowComponent) ||
      (tableConfigs && tableConfigs.customRows && tableConfigs.customRows.component) ||
      DefaultTableComponents.row;
  }
}
