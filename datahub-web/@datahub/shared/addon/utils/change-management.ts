import { IChangeLogProperties, ChangeLog } from '@datahub/shared/modules/change-log';
import { msTimeAsUnix } from '@datahub/utils/helpers/ms-time-as-unix';
import { IAddChangeLogModalProps } from '@datahub/shared/types/change-management/change-log';
import { OwnerUrnNamespace } from '@datahub/data-models/constants/entity/dataset/ownership';
import getActorFromUrn from '@datahub/data-models/utils/get-actor-from-urn';

type IDetailResponse = Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement;

export const defaultChangeLogProperties: IChangeLogProperties = {
  id: 0,
  subject: '',
  createdBy: '',
  dateAdded: 0,
  content: '',
  sendEmail: false,
  owningEntity: { dataset: '' }
};
/**
 * Helper method to massage `detailApiResponse` into a format more conducive to UI side of things.
 * @param id ID of the ChangeLog
 * @param detailResponse detail response of the changeLog
 */
export const transformChangeLogDetailResponse = (detailResponse: IDetailResponse): IChangeLogProperties => {
  return Object.entries(detailResponse).reduce(
    (transformedResponse: IChangeLogProperties, [key, value]): IChangeLogProperties => {
      switch (key) {
        case 'message':
          return {
            ...transformedResponse,
            subject: value?.subject,
            content: value?.messageText
          };
          break;
        case 'lastModified':
          return {
            ...transformedResponse,
            createdBy: getActorFromUrn(value?.actor),
            dateAdded: msTimeAsUnix([value?.time])
          };
          break;
        // In a save only state this property is absent on the response.
        case 'notification':
          return {
            ...transformedResponse,
            recipients: value?.recipients,
            sendEmail: value?.recipients.length > 0
          };
          break;
        case 'owningEntity':
          return {
            ...transformedResponse,
            owningEntity: value
          };
          break;
        case 'id':
          return {
            ...transformedResponse,
            id: value
          };
          break;
        default:
          return transformedResponse;
          break;
      }
    },
    defaultChangeLogProperties
  );
};

/**
 * Helper method meant to extract the user entered information and construct a payload that is more suitable for making an api call.
 */
export const constructChangeLogContent = (
  newChangeLogInfo: IAddChangeLogModalProps,
  username: string,
  urn: string,
  entityType: string
): Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagementContent => {
  const { subject, content, sendEmail, recipients } = newChangeLogInfo;

  const owningEntity = {
    [entityType]: urn
  } as Com.Linkedin.DataConstructChangeManagement.OwningEntity;
  let changeLogContent: Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagementContent = {
    lastModified: {
      actor: `${OwnerUrnNamespace.corpUser}:${username}`,
      time: Date.now()
    },
    message: {
      messageText: content,
      subject
    },
    // Default category of Other will be sent to the api call,
    // until user is allowed to specify this from the UI
    category: 'OTHER',
    owningEntity
  };

  if (sendEmail) {
    const notification: Com.Linkedin.DataConstructChangeManagement.Notification = {
      recipients: recipients ? recipients : [],
      notificationTypes: {
        jira: false,
        email: true,
        banner: false
      }
    };
    changeLogContent = { ...changeLogContent, notification };
  }

  return changeLogContent;
};

/**
 * Helper method to map followers to recipients.
 *
 * @param followers List of followers for a given entity
 */
export const transformFollowersIntoRecipients = (
  followers: Array<Com.Linkedin.Common.FollowerType>
): Array<Com.Linkedin.DataConstructChangeManagement.NotificationRecipient> =>
  followers.reduce(
    (
      recipients: Array<Com.Linkedin.DataConstructChangeManagement.NotificationRecipient>,
      follower: Com.Linkedin.Common.FollowerType
    ) => {
      if (follower?.corpUser) {
        recipients = [...recipients, { userUrn: follower.corpUser }];
      }
      if (follower?.corpGroup) {
        recipients = [...recipients, { groupUrn: follower.corpGroup }];
      }
      return recipients;
    },
    []
  );

/**
 * Assembly method that transforms `changeLog class` instances into API friendly responses for `updateChangeLog()`
 *
 * @param changeLog The change log that needs transforming
 * @param username The username of the author of the changelog
 */
export const transformChangeLogIntoResponse = (changeLog: ChangeLog, username: string): IDetailResponse => ({
  id: changeLog.id,
  owningEntity: changeLog.owningEntity,
  category: 'OTHER',
  message: {
    subject: changeLog.subject,
    messageText: changeLog.content
  },
  lastModified: {
    actor: `${OwnerUrnNamespace.corpUser}:${username}`,
    time: Date.now()
  },
  notification: {
    recipients: changeLog?.recipients || [],
    notificationTypes: {
      jira: false,
      email: true,
      banner: false
    }
  }
});
