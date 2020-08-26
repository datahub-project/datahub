import Component from '@ember/component';
import { noop } from 'lodash';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/change-management/change-log-table';
import { layout } from '@ember-decorators/component';
import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';
import { ChangeLog } from '@datahub/shared/modules/change-log';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { computed } from '@ember/object';
import { inject as service } from '@ember/service';

export const baseTableClass = 'change-log-table';
export const tableTitle = 'Change Log';

/**
 * Presentational component responsible for displaying the ChangeLogs associated with an entity in a Tabular format.
 * The logs collectively form the `Audit Log` . For more info - refer to `go/datahub/changemanagementdd`
 *
 * The component uses NachoTable behind the scenes and provides functionality to
 * 1: Add a new log
 * 2: View Details of an existing log
 * 3: Send out an E-mail for a log that has not been published
 */
@layout(template)
export default class ChangeLogTable extends Component {
  /**
   * Styling classname for the table
   */
  baseTableClass = baseTableClass;

  /**
   * Title of the table
   */
  tableTitle = tableTitle;

  /**
   * The Change logs being passed in from the container that needs to be displayed in the table
   */
  changeLogs: Array<ChangeLog> = [];

  /**
   * Flag indicating if current user can only read the Audit log or has rights to create a new one / send out notifications as well.
   */
  readOnly = true;

  /**
   * Tooltip text that provides user with contextual info around change management
   */
  tooltipText = 'An audit log of all the changes logged by owners. Click here to learn more about Change Management';

  /**
   * Configuration properties to render the changeLogs present as a table
   */
  tableConfigs: INachoTableConfigs<{}> = {
    labels: ['createdBy, dateAdded, type, actions'],
    useBlocks: { body: true, footer: true },
    headers: [
      { title: 'Created by', className: `${baseTableClass}__created-by` },
      { title: 'Date Added', className: `${baseTableClass}__date` },
      { title: 'Subject', className: `${baseTableClass}__subject` },
      { title: 'Notification Status', className: `${baseTableClass}__notification-status` },
      { title: '', className: `${baseTableClass}__actions` }
    ]
  };

  /**
   * Application configurator is used to extract the changeManagement link from wikiLinks config object
   */
  @service
  configurator!: IConfigurator;

  /**
   * Reference to the Wiki link help resource for Change Management
   */
  @computed('configurator')
  get changeManagementWiki(): string {
    return this.configurator.getConfig('wikiLinks').changeManagement || '';
  }

  /**
   * External action handler when a new ChangeLog needs to be added
   */
  onAddLog: () => void = noop;

  /**
   * Placeholder for external action handler that is invoked when "View Detail" is clicked
   */
  showChangeLogDetail: (id: number) => void = noop;

  /**
   * Placeholder for external action handler that is invoked when "Send Email" is clicked
   */
  onSendEmail: (id: number) => void = noop;
}
