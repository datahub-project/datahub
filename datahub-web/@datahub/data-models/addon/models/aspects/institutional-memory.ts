import { IInstitutionalMemory } from '@datahub/metadata-types/types/aspects/institutional-memory';
import { oneWay } from '@ember/object/computed';
import { computed } from '@ember/object';
import { msTimeAsUnix } from '@datahub/utils/helpers/ms-time-as-unix';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

export class InstitutionalMemory {
  /**
   * Holds the original data held by this institutional memory class
   */
  _data: IInstitutionalMemory;

  /**
   * Returns the link related to this institutional memory object
   * @type {string}
   */
  @oneWay('_data.url')
  url!: string;

  /**
   * Returns the description related to this institutional memory object
   * @type {string}
   */
  @oneWay('_data.description')
  description!: string;

  /**
   * Returns the timestmap in unix format from the create stamp of this institutional
   * memory object, if it exists
   * @type {number | undefined}
   */
  @computed('_data.createStamp')
  get timestamp(): number | undefined {
    const { _data } = this;

    if (!_data.createStamp) {
      return;
    }

    return msTimeAsUnix([_data.createStamp.time]);
  }

  /**
   * Returns the actor for this institutional memory object, without the actor urn prefix
   * (if there is one)
   * @type {string | undefined}
   */
  @computed('_data.createStamp')
  get actor(): PersonEntity | undefined {
    const { _data } = this;

    if (!_data.createStamp) {
      return;
    }

    return new PersonEntity(_data.createStamp.actor);
  }

  /**
   * Reads the institutional memory working by reconstructing it from the elements
   * of the class
   */
  readWorkingCopy(): IInstitutionalMemory {
    const { url, description, _data } = this;
    const workingCopy: IInstitutionalMemory = { url, description };

    if (_data.createStamp) {
      workingCopy.createStamp = { ..._data.createStamp };
    }

    return workingCopy;
  }

  constructor(_data: IInstitutionalMemory) {
    this._data = _data;
  }
}

export type InstitutionalMemories = Array<InstitutionalMemory>;
