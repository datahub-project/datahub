import { IAddChangeLogModalProps } from '@datahub/shared/types/change-management/change-log';
import marked from 'marked';

/**
 * The local attributes of the class. They mirror the UI representation of how a changeLog looks.
 */
export interface IChangeLogProperties extends IAddChangeLogModalProps {
  // ID of the Change Log
  id: number;
  // LDAP of the owner who created the log
  createdBy: string;
  // Date when the log was added
  dateAdded: Com.Linkedin.Common.Time;
  // Entity that owns this Change log
  owningEntity: Com.Linkedin.DataConstructChangeManagement.OwningEntity;
}

/**
 * ChangeLog models the attributes for a Change Management Log
 *
 * @class DatasetOwnership
 */
export class ChangeLog implements IChangeLogProperties {
  /**
   * Refer to `IChangeLogProperties` for docs on each of these local properties below in this Class
   */

  id: number;
  createdBy: string;
  dateAdded: Com.Linkedin.Common.Time;
  subject: string;
  content: string;
  sendEmail: boolean;
  recipients?: Array<Com.Linkedin.DataConstructChangeManagement.NotificationRecipient>;
  owningEntity: Com.Linkedin.DataConstructChangeManagement.OwningEntity;

  constructor(changeLogArgs: IChangeLogProperties) {
    const { id, createdBy, dateAdded, subject, content, sendEmail, recipients, owningEntity } = changeLogArgs;
    this.id = id;
    this.createdBy = createdBy;
    this.dateAdded = dateAdded;
    this.subject = subject;
    this.content = marked(content).htmlSafe();
    this.sendEmail = sendEmail;
    this.recipients = recipients;
    this.owningEntity = owningEntity;
  }
}
