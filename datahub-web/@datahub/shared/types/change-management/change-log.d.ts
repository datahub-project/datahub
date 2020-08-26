/**
 * The type defining the local properties being captured on the `Add-Change-Log-Modal`
 */
export interface IAddChangeLogModalProps {
  // The subject of the notification ( eg - email subject)
  subject: string;
  // The bulk of the Change log consisting the actual information the Owner is trying to broadcast
  content: string;
  // A flag indicating if the Owner wishes to send a notification or simply save it to the `Audit log`
  sendEmail: boolean;
  // Array of recipients for whom the Email is intended
  recipients?: Array<Com.Linkedin.DataConstructChangeManagement.NotificationRecipient>;
}
