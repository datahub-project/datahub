import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/tables/simple-record';
import { layout } from '@ember-decorators/component';
import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';

// The default top level class for the simple record component, may be overridden on the component
const baseClass = 'simple-record-table';

/**
 * SimpleRecord is presentational component for rendering a table of records in a simple two column layout of attributes and values
 * @export
 * @class TablesSimpleRecord
 * @extends {Component}
 */
@layout(template)
export default class TablesSimpleRecord extends Component {
  /**
   * CSS top level class applied to the component
   */
  baseClass = baseClass;

  /**
   * The data to be rendered in a two column layout of attributes and values
   */
  entries?: Record<string, string | number>;

  /**
   * Configuration properties for rendering a NachoTable for the record entries
   */
  tableConfigs: INachoTableConfigs<string> = {
    useBlocks: { body: true, header: false },
    labels: []
  };

  /**
   * Default message when there are no entries to be shown
   */
  emptyStateHeader = 'We are unable to show any entries for this record';

  /**
   * By default, this.emptyStateHeader will be rendered within the simple record table and the provided text or default text in emptyStateHeader.
   * If, however, the component should not be shown when there are no results, this flag can be set to false. This will render just the empty state
   */
  includeTitleWhenEmpty = true;
}
