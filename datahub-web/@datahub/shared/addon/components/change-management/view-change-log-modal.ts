import Component from '@glimmer/component';
import { ChangeLog } from '@datahub/shared/modules/change-log';

// Styling class name for template
export const baseModalClass = 'view-change-log';

interface IViewChangeLogModalArgs {
  // External handler method for handling the closing of the modal
  onCloseModal: () => void;

  // The changeLog being viewed
  changeLog: ChangeLog;
}

/**
 * Presentational Component that displays details of a ChangeLog
 */
export default class ViewChangeLogModal extends Component<IViewChangeLogModalArgs> {
  /**
   * CSS Styling name for easier access in template
   */
  baseModalClass = baseModalClass;

  /**
   * Getter to check if the Change log was used to send notification or not
   */
  get notificationText(): string {
    return this.args.changeLog.sendEmail ? 'Sent' : 'Not Sent';
  }

  /**
   * Formats the recipients into Displayable text required for the `Audience section of the Modal`
   */
  get audienceText(): string {
    const recipients = this.args.changeLog.recipients;
    return `${recipients?.length || 0} recipients`;
  }
}
