import Component from '@ember/component';
import { computed } from '@ember/object';
import { empty } from '@ember/object/computed';
import { classNames } from '@ember-decorators/component';
import { OwnerWithAvatarRecord } from 'datahub-web/typings/app/datasets/owners';

@classNames('dataset-authors-suggested')
export default class DatasetsOwnersSuggestedOwners extends Component {
  /**
   * Whether or not the component is expanded. If not, users will only see the initial header information
   * whereas if expanded then users will see the list of all suggested owners
   * @type {boolean}
   */
  isExpanded = false;

  /**
   * Passed in value from parent component, `dataset-authors`, a.k.a. systemGeneratedOwners, this list
   * represents a possible list of owners provided by scanning various systems.
   * @type {Array<OwnerWithAvatarRecord>}
   * @default []
   */
  owners: Array<OwnerWithAvatarRecord> = [];

  /**
   * Computed based on the owners array, detects whether this array is empty or not
   * @type {ComputedProperty<boolean>}
   */
  @empty('owners')
  isEmpty: boolean;

  /**
   * For the facepile in the suggestions window header, we do not need tos how all the faces of all the
   * possible owners as this could be a large amount. Take only up to the first four to pass into the
   * template for rendering
   * @type {ComputedProperty<Array<OwnerWithAvatarRecord>>}
   */
  @computed('owners')
  get facepileOwners(): Array<OwnerWithAvatarRecord> {
    return this.owners.slice(0, 4);
  }
}
