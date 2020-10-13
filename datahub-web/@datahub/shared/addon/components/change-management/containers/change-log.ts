import Component from '@ember/component';
import CurrentUser from '@datahub/shared/services/current-user';
import { inject as service } from '@ember/service';
import { alias, map } from '@ember/object/computed';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/change-management/containers/change-log';
import { layout } from '@ember-decorators/component';
import { task } from 'ember-concurrency';
import { action, set, setProperties, computed } from '@ember/object';
import { IAddChangeLogModalProps } from '@datahub/shared/types/change-management/change-log';
import { getChangeLog, createChangeLog, updateChangeLog } from '@datahub/shared/api/change-management/change-log';
import { ChangeLog, IChangeLogProperties } from '@datahub/shared/modules/change-log';
import {
  transformChangeLogDetailResponse,
  constructChangeLogContent,
  transformFollowersIntoRecipients,
  transformChangeLogIntoResponse
} from '@datahub/shared/utils/change-management';
import { ETask, ETaskPromise } from '@datahub/utils/types/concurrency';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { IDataModelEntitySearchResult, ISearchDataWithMetadata } from '@datahub/data-models/types/entity/search';
import { DataConstructChangeManagementEntity } from '@datahub/data-models/entity/data-construct-change-management/data-construct-change-management-entity';
import { singularize } from 'ember-inflector';
import { noop } from 'lodash';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';
import Notifications from '@datahub/utils/services/notifications';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import DataModelsService from '@datahub/data-models/services/data-models';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

/**
 * Container component that handles fetching of Data ( Followers , Search results )
 * Massages them appropriately , instantiates the change log objects and passes required presentational information over to the children.
 */
@layout(template)
@containerDataSource<ChangeLogContainer>('getContainerDataTask', ['entity', 'results'])
export default class ChangeLogContainer extends Component {
  /**
   * Injects the config service
   */
  @service
  configurator!: IConfigurator;

  /**
   * Reference to the app notification service. Users are notified on fetching of changeLog detail and creation of a new changeLog
   */
  @service
  notifications!: Notifications;

  /**
   * Reference to the data models service in order to access the proper entity for our current user
   */
  @service
  dataModels!: DataModelsService;

  /**
   * The data entity that gives context to this container regarding the change logs
   * A Passed in property from parent
   */
  entity?: DataModelEntityInstance;

  /**
   * Flag indicating if a user is currently adding a new change log.
   */
  isAddingChangeLog = false;

  /**
   * Flag indicating if a user is currently viewing change log detail info
   */
  isViewingChangeLog = false;

  /**
   * Flag indicating if a user is currently sending email for a previously unsent change log
   */
  isSendingEmailOnly = false;

  /**
   * Current Changelog whose detail info is being viewed
   */
  currentChangeLog?: ChangeLog;

  /**
   * A list of users that will receive notifications regarding this entity if an owner wishes to publish a changelog.
   */
  recipients: Array<Com.Linkedin.DataConstructChangeManagement.NotificationRecipient> = [];

  /**
   * The search results containing all the change logs that belong to this entity
   * A Passed in property from parent
   */

  results?: IDataModelEntitySearchResult<ISearchDataWithMetadata<DataConstructChangeManagementEntity>>;

  /**
   * The owners for the entity
   */
  @computed('entity.owners')
  get owners(): Array<Com.Linkedin.Common.Owner> {
    return this.entity?.owners || [];
  }

  /**
   * Constructs a map of userNames of each owner
   */
  @map('owners', function(this: ChangeLogContainer, owner: Com.Linkedin.Common.Owner): string {
    return owner.owner;
  })
  ownerUserNames!: Array<string>;

  /**
   * Returns if the currently logged in user is an enlisted owner or not
   */
  @computed('entity.owners')
  get isCurrentUserAnOwner(): boolean {
    return this.ownerUserNames.includes(this.username || '');
  }

  /**
   * Converts the array of usernames into an array of recipients
   */
  @computed('entity.owners')
  get ownersAsRecipients(): Array<Com.Linkedin.DataConstructChangeManagement.NotificationRecipient> {
    const { ownerUserNames, dataModels } = this;
    const PersonEntityClass = dataModels.getModel(PersonEntity.displayName);
    return ownerUserNames.map(userName => {
      return {
        userUrn: PersonEntityClass.urnFromUsername(userName)
      };
    });
  }

  /**
   * Sorts the change log responses based on the `lastModified` field
   */
  @computed('results')
  get sortedResultResponses(): Array<Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement> {
    const { results } = this;
    const dataArray = results?.data || [];
    const dataEntities: Array<DataConstructChangeManagementEntity> = dataArray.mapBy('data');
    const resultResponses: Array<Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement> = dataEntities.mapBy(
      'entity'
    );
    return resultResponses.sort((responseA, responseB) => responseB.lastModified.time - responseA.lastModified.time);
  }

  /**
   * Gets the flag guard for showing change management related feature components
   * @default false defaulted to not show the feature
   */
  @computed('configurator.showDataConstructChangeManagement')
  get showDataConstructChangeManagement(): boolean {
    const { configurator } = this;
    return (
      configurator && configurator.getConfig('showDataConstructChangeManagement', { useDefault: true, default: false })
    );
  }

  /**
   * A list of all the changeLogs that are being handled / displayed on the UI side.
   * This is the massaged / extracted version of the `results` ( the search response)
   *
   * The mapping logic extracts the response , transforms it into a `ChangeLog` object that presentational components can understand.
   */
  @map('sortedResultResponses', function(
    this: ChangeLogContainer,
    sortedResultResponse: Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement
  ): ChangeLog | void {
    return new ChangeLog(transformChangeLogDetailResponse(sortedResultResponse));
  })
  changeLogs!: Array<ChangeLog>;

  /**
   * References the CurrentUser service
   */
  @service('current-user')
  currentUser!: CurrentUser;

  /**
   * The current user's username
   */
  @alias('currentUser.entity.username')
  username?: string;

  /**
   * External placeholder method that gets invoked upon a save operation complete
   */
  onSaveComplete: () => void = noop;

  /**
   * Fetches the change log whose detail is to be shown and opens the modal to display it.
   * @param id ID of the change log, whose detail is to be shown
   */
  @action
  showChangeLogDetail(id: number): void {
    const currentChangeLog: ChangeLog | undefined = this.changeLogs.find(changeLog => changeLog.id === id);
    set(this, 'currentChangeLog', currentChangeLog);
    set(this, 'isViewingChangeLog', true);
  }

  /**
   * Creates a new `ChangeLog` object by instantiating a native class with properties and appends it to the `changeLogs` list
   * @param changeLogResponse
   */
  instantiateChangeLog(changeLogResponse: IChangeLogProperties): void {
    const changeLog = new ChangeLog(changeLogResponse);
    this.changeLogs.pushObject(changeLog);
  }

  /**
   * Method that extracts the changelog to be sent out and activates the `add-change-log-modal`, so that the user can verify recipients
   *
   * @param id ID of the changelog whose notification is being sent out
   */
  @action
  onSendEmail(id: number): void {
    const currentChangeLog: ChangeLog | undefined = this.changeLogs.find(changeLog => changeLog.id === id);
    setProperties(this, {
      currentChangeLog,
      isSendingEmailOnly: true,
      isAddingChangeLog: true
    });
  }

  /**
   * Enables viewing of the Change Log modal
   */
  @action
  onAddLog(): void {
    set(this, 'isAddingChangeLog', true);
  }

  /**
   * Handles saving of a new changeLog
   * @param changeLogInfo All the required info for a new changeLog creation
   */
  @action
  async onSave(changeLogInfo: IAddChangeLogModalProps): Promise<void> {
    //await operation of save
    try {
      await this.createChangeLog.perform(changeLogInfo);
      this.notifications.notify({
        content: 'Successfully created a new Change Log. Your changes will be updated in the table shortly.',
        type: NotificationEvent.success
      });
    } catch (e) {
      this.notifications.notify({
        content: `We could not create a new Change Log. ${e}`,
        type: NotificationEvent.error
      });
    }
    this.onSaveComplete();
  }

  /**
   * Handles assimilation of the `currentChangeLog` and `recipients` to be sent to,
   * and invokes the task that performs the actual operation.
   *
   * @param recipients Users who will receive the email for the changeLog
   */
  @action
  async onSendEmailOnly(
    recipients: Array<Com.Linkedin.DataConstructChangeManagement.NotificationRecipient>
  ): Promise<void> {
    const { currentChangeLog } = this;
    if (currentChangeLog) {
      const translatedChangeLog = Object.assign({}, currentChangeLog, { content: currentChangeLog.content.toString() });
      const changeLogToBeUpdated: ChangeLog = Object.assign(
        {},
        translatedChangeLog,
        { recipients },
        { sendEmail: true }
      );
      try {
        await this.updateChangeLog.perform(changeLogToBeUpdated);
        this.notifications.notify({
          content: 'Notification sent out successfully.',
          type: NotificationEvent.success
        });
      } catch (e) {
        this.notifications.notify({
          content: `We could not send out the notification. ${e}`,
          type: NotificationEvent.error
        });
      }
      this.onSaveComplete();
    }
  }

  /**
   * Disables viewing of the Add change log Modal
   */
  @action
  handleClosingOfAddChangeLogModal(): void {
    setProperties(this, {
      isAddingChangeLog: false,
      isSendingEmailOnly: false
    });
  }

  /**
   * Disables viewing of the View Change Log modal
   */
  @action
  handleClosingOfViewChangeLogModal(): void {
    set(this, 'isViewingChangeLog', false);
  }

  /**
   * Container to get the Detail response for a particular Change Management Log
   *
   * @param id : The identifier of the Change Log
   */
  @(task(function*(
    id: number
  ): IterableIterator<Promise<Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement>> {
    return ((yield getChangeLog(
      id
    )) as unknown) as Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement;
  }).restartable())
  getChangeLogDetail!: ETask<Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement, number>;

  /**
   * Task that creates a new changeLog
   */
  @(task(function*(this: ChangeLogContainer, changeLogInfo: IAddChangeLogModalProps): IterableIterator<Promise<void>> {
    const { entity, username } = this;
    if (entity && username) {
      const urn = entity.urn || '';
      const entityType = singularize(entity.displayName);
      const changeLogContent: Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagementContent = constructChangeLogContent(
        changeLogInfo,
        username,
        urn,
        entityType
      );
      ((yield createChangeLog(changeLogContent)) as unknown) as void;
    }
  }).drop())
  createChangeLog!: ETask<void, IAddChangeLogModalProps>;

  /**
   * Task that updates an existing changeLog
   */
  @(task(function*(this: ChangeLogContainer, changeLog: ChangeLog): IterableIterator<Promise<void>> {
    const { entity, username } = this;
    if (entity && username) {
      const changeLogResponse = transformChangeLogIntoResponse(changeLog, username);
      ((yield updateChangeLog(changeLog.id, changeLogResponse)) as unknown) as void;
    }
  }).drop())
  updateChangeLog!: ETask<void, IAddChangeLogModalProps>;

  /**
   * Task defined to get the necessary data for this container.
   */
  @(task(function*(this: ChangeLogContainer): IterableIterator<Promise<void>> {
    const { entity, ownersAsRecipients } = this;
    if (entity) {
      ((yield entity.readFollows()) as unknown) as Promise<void>;

      // 1. Gather unique recipient urns
      const uniqueRecipientUrns = new Set(
        [
          ...transformFollowersIntoRecipients(entity.follow?.followers.map(followers => followers.follower) || []),
          ...ownersAsRecipients
        ].map(user => (user as { userUrn?: string }).userUrn || '')
      );

      // 2. Construct recipients from the recipient Urns
      const recipients: Array<Com.Linkedin.DataConstructChangeManagement.NotificationRecipient> = Array.from(
        uniqueRecipientUrns
      ).map((urn: string) => ({
        userUrn: urn
      }));

      // 3 . Set the recipients locally for sending notifications
      set(this, 'recipients', recipients);
    }
  }).drop())
  getContainerDataTask!: ETaskPromise<void>;
}
