import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../templates/components/institutional-memory/containers/tab';
import { layout } from '@ember-decorators/component';
import { action, computed } from '@ember/object';
import { containerDataSource } from '@datahub/utils/api/data-source';
import { isEqual } from 'lodash';
import { InstitutionalMemory, InstitutionalMemories } from '@datahub/data-models/models/aspects/institutional-memory';
import { run, schedule } from '@ember/runloop';
import { task } from 'ember-concurrency';
import { ETaskPromise } from '@datahub/utils/types/concurrency';
import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { changeManagementEnabledEntityList } from '@datahub/shared/constants/change-management';

@layout(template)
@containerDataSource<InstitutionalMemoryContainersTab>('getContainerDataTask', ['entity'])
export default class InstitutionalMemoryContainersTab extends Component {
  /**
   * The entity for which we are instantiating this institutional memory tab. It is our method of
   * actually accessing the data layer in order to read and write the institutional memory objects
   */
  entity?: DataModelEntityInstance;

  /**
   * The read or modified institutional memory links, defaults to undefined, so that we don't flash
   * the default empty state before the api call resolves.
   */
  @computed('entity.institutionalMemories')
  get institutionalMemoryList(): InstitutionalMemories | undefined {
    const { entity } = this;
    return entity && entity.institutionalMemories;
  }

  /**
   * Flag that indicates if the current entity belongs to a allow list that dictates if Change management is enabled or not.
   */
  @computed('entity.displayName')
  get isChangeManagementEnabled(): boolean {
    const { entity } = this;
    return changeManagementEnabledEntityList.includes(entity?.displayName || '');
  }
  /**
   * This container data task runs when the component is initially rendered or if the entity given
   * to the component has changed
   */
  @(task(function*(this: InstitutionalMemoryContainersTab): IterableIterator<Promise<InstitutionalMemories>> {
    const { entity } = this;

    if (!entity) {
      return;
    }

    // Note: Using Ember runloop here as there seems to be an issue with the normal yield logic
    // where a rare timing issue can lead to institutional memory staying undefined and not
    // updating to the response, which leads to a blank tab. Runloop forces Ember to adhere to our
    // timing of a runloop start/end
    run((): void => {
      schedule(
        'actions',
        async (): Promise<void> => {
          // Assumes that the entity supports institutional memory, otherwise this will throw an error
          // for not implemented yet.
          await entity.readInstitutionalMemory();
        }
      );
    });
  }).drop())
  getContainerDataTask!: ETaskPromise<InstitutionalMemories>;
  /**
   * This task is used to actually save user changes to the entity's institutional memory list
   */
  @(task(function*(this: InstitutionalMemoryContainersTab): IterableIterator<Promise<InstitutionalMemories | void>> {
    const { entity } = this;

    if (!entity) {
      return;
    }

    yield entity.writeInstitutionalMemory();

    yield this.getContainerDataTask.perform();
  }).drop())
  writeContainerDataTask!: ETaskPromise<InstitutionalMemories | void>;
  /**
   * Triggers the task to save the institutional memory state
   */
  saveInstitutionalMemoryList(): void {
    this.writeContainerDataTask.perform();
  }

  /**
   * Adds and saves a link given by the user for the institutional memory list
   * @param {InstitutionalMemory} linkObject - the link object created that should be added to our
   *  institutional memory link list
   */
  @action
  addInstitutionalMemoryLink(linkObject: InstitutionalMemory): void {
    const { institutionalMemoryList } = this;
    if (institutionalMemoryList) {
      institutionalMemoryList.addObject(linkObject);
      this.saveInstitutionalMemoryList();
    }
  }

  /**
   * Removes and saves the new state for the given institutional memory list
   * @param {InstitutionalMemory} linkObject - link object determined by the user should be removed
   *  from our list
   */
  @action
  removeInstitutionalMemoryLink(linkObject: InstitutionalMemory): void {
    const { institutionalMemoryList } = this;
    if (institutionalMemoryList) {
      const removalIndex = institutionalMemoryList.findIndex((link): boolean => isEqual(link, linkObject));

      if (removalIndex > -1) {
        institutionalMemoryList.removeAt(removalIndex);
        this.saveInstitutionalMemoryList();
      }
    }
  }
}
