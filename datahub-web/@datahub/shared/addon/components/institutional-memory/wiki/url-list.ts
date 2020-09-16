import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/institutional-memory/wiki/url-list';
import { layout, tagName, classNames } from '@ember-decorators/component';
import { action, computed } from '@ember/object';
import { noop } from 'lodash';
import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';
import { set } from '@ember/object';
import { NotificationEvent } from '@datahub/utils/constants/notifications';
import { notificationDialogActionFactory } from '@datahub/utils/lib/notifications';
import { singularize } from 'ember-inflector';
import { InstitutionalMemory } from '@datahub/data-models/models/aspects/institutional-memory';
import Notifications from '@datahub/utils/services/notifications';
import { inject as service } from '@ember/service';

export const baseTableClass = 'institutional-memory-links';

@layout(template)
@tagName('section')
@classNames(`${baseTableClass}__container`)
export default class InstitutionalMemoryWikiUrlList extends Component {
  /**
   * Passed in notification service that can help us open a dialog in response to user actions such as desire
   * to override a readonly field
   */
  @service('notifications')
  notificationsService?: Notifications;

  /**
   * Storing the base table class on the component for ease of access in the template
   * @type {string}
   */
  baseTableClass = baseTableClass;

  /**
   * Static configurations for the nacho-table component. Helps us not have to worry about
   * rendering the headers ourselves
   */
  tableConfigs: INachoTableConfigs<InstitutionalMemory> = {
    labels: ['name', 'date', 'url', 'description', 'rowactions'],
    useBlocks: { body: true, footer: true },
    headers: [
      { title: 'Author', className: `${baseTableClass}__author` },
      { title: 'Date Added', className: `${baseTableClass}__date` },
      { title: 'URL', className: `${baseTableClass}__link` },
      { title: 'Description', className: `${baseTableClass}__description` },
      { title: '', className: `${baseTableClass}__actions` }
    ]
  };

  /**
   * Passed in displayName for the entity that is providing the context for this institutional
   * memory
   */
  entityDisplayName?: string;

  /**
   * Passed in list data for the institutional memory links
   */
  listData?: Array<InstitutionalMemory>;

  /**
   * Passed in method to create an insitutional memory link and save these changes
   */
  addInstitutionalMemoryLink: (link: InstitutionalMemory) => void = noop;

  /**
   * Passed in method to remove an institutional memory link and save those changes
   */
  removeInstitutionalMemoryLink: (link: InstitutionalMemory) => void = noop;

  /**
   * Flag determining whether or not we are going to show the pop up modal related to adding
   * a link
   */
  isShowingModal = false;

  /**
   * If we have a display name given for the entity for which this institutional memory component
   * is given context, then we create a singularized version of that to show in the empty state
   * message
   * @type {string}
   */
  @computed('entityDisplayName')
  get entityName(): string {
    const { entityDisplayName: displayName } = this;
    return (displayName && singularize(displayName)) || '';
  }

  /**
   * Activates a user flow that should result in the creation of an institutional memory
   * wiki link
   */
  @action
  onAddLink(): void {
    set(this, 'isShowingModal', true);
  }

  /**
   * Triggered by the user when they do something that cancels the add link process (includes dismiss,
   * cancel button, or clicking outside the modal)
   */
  @action
  onCancelAddLink(): void {
    set(this, 'isShowingModal', false);
  }

  /**
   * Finalizes the user action to save a link by creating the desired link object and then triggering
   * the container level action to actually persist this information to the data layer.
   * @param url - url from user input for the link
   * @param description - description from user input for the link
   */
  @action
  onSaveLink(url: string, description: string): void {
    const linkObject: InstitutionalMemory = new InstitutionalMemory({ url, description });
    this.addInstitutionalMemoryLink(linkObject);
  }

  /**
   * Activates a user flow that should result in the removal of an institutional memory
   * wiki link
   * @param {InstitutionalMemory} linkObject - Link that the user desires to remove
   */
  @action
  async onRemoveLink(linkObject: InstitutionalMemory): Promise<void> {
    const { dialogActions, dismissedOrConfirmed } = notificationDialogActionFactory();
    const { notificationsService, removeInstitutionalMemoryLink } = this;

    if (notificationsService && notificationsService.notify) {
      notificationsService.notify({
        header: 'Remove link',
        type: NotificationEvent.confirm,
        content: 'The link will be permanently removed from the page. Are you sure you want to do that?',
        dismissButtonText: 'Cancel',
        confirmButtonText: 'Delete',
        dialogActions
      });

      try {
        await dismissedOrConfirmed;
      } catch {
        return;
      }
    }

    removeInstitutionalMemoryLink(linkObject);
  }
}
