import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/nacho-table';
import { classNames } from '@ember-decorators/component';
import { computed } from '@ember/object';
import { DefaultTableComponents, DefaultTableClasses } from '../constants/default-table-properties';
import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';
import { not } from '@ember/object/computed';

@classNames(DefaultTableClasses.container)
export default class NachoTable<T> extends Component {
  layout = layout;

  /**
   * Default component paths for the different parts of the table
   * @type {DefaultTableComponents}
   */
  tableHeaderComponent = DefaultTableComponents.header;
  tableBodyComponent = DefaultTableComponents.body;
  tableRowComponent = DefaultTableComponents.row;
  tableFooterComponent = DefaultTableComponents.footer;
  tableGlobalComponent = DefaultTableComponents.global;

  /**
   * Passed in table data. This should be an array of objects that represent each row. The
   * objects themselves can have any interface deemed appropriate by the consumer.
   * @type {Array<T>}
   */
  data!: Array<T>;

  /**
   * Passed in table configurations. This is the source of truth that helps us construct a table
   * based on the consumer needs and should be generic and highly customizable.
   * @type {INachoTableConfigs}
   */
  tableConfigs!: INachoTableConfigs<T>;

  /**
   * Additional table classes passed in as a string to be added to the actual table itself. Helpful
   * for when the host application wants to add overrides to the base nacho stylings or simply for
   * BEM purposes
   * @type {string}
   */
  tableClass!: string;

  @computed('tableClass')
  get tableClasses(): { main: string; global: string } {
    const baseClass = 'nacho-table';
    const tableClass = this.tableClass;

    const tableClasses = {
      main: baseClass,
      global: `${baseClass}__global`
    };

    if (tableClass) {
      tableClasses.main += ` ${tableClass}`;
      tableClasses.global += ` ${tableClass
        .split(' ')
        .map(item => `${item}__global`)
        .join(' ')}`;
    }

    return tableClasses;
  }

  /**
   * Quick computation to determine whether or not we have any table configs
   */
  @not('tableConfigs')
  noConfigs!: boolean;

  /**
   * Initial computation for our fields. Can be expanded over time as our table operations grow
   * in complexity
   * @type {ComputedProperty<NachoTable['data']>}
   */
  @computed('data')
  get fields(): NachoTable<T>['data'] {
    return this.data || [];
  }
}
