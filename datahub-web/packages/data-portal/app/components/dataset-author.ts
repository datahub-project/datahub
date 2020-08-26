import Component from '@ember/component';

import { IOwner } from 'datahub-web/typings/api/datasets/owners';
import { OwnerSource, OwnerType } from 'datahub-web/utils/api/datasets/owners';
import { action, computed } from '@ember/object';
import { OwnerWithAvatarRecord } from 'datahub-web/typings/app/datasets/owners';
import { tagName, classNames, className } from '@ember-decorators/component';
import { equal, not } from '@ember/object/computed';

/**
 * This component renders a single owner record and also provides functionality for interacting with the component
 * in the ui or performing operations on a single owner record
 * @export
 * @class DatasetAuthor
 * @extends {Component}
 */
@tagName('tr')
@classNames('dataset-author-record')
export default class DatasetAuthor extends Component {
  /**
   * The owner record being rendered
   * @type {OwnerWithAvatarRecord}
   * @memberof DatasetAuthor
   */
  owner: OwnerWithAvatarRecord;

  /**
   * List of suggested owners that have been confirmed by a user
   * @type {Array<IOwner>}
   * @memberof DatasetAuthor
   */
  commonOwners: Array<IOwner>;

  /**
   * External action to handle owner removal from the confirmed list
   * @param {IOwner} owner the owner to be removed
   * @memberof DatasetAuthor
   */
  removeOwner: (owner: IOwner) => IOwner | void;

  /**
   * External action to handle owner addition to the confirmed list
   * @param {IOwner} owner the suggested owner to be confirmed
   * @return {Array<IOwner> | void} the list of owners or void if unsuccessful
   * @memberof DatasetAuthor
   */
  confirmSuggestedOwner: (owner: IOwner) => Array<IOwner> | void;

  /**
   * External action to handle owner property updates, currently on the confirmed list
   * @param {IOwner} owner the owner to update
   * @param {OwnerType} type the type of the owner
   * @memberof DatasetAuthor
   */
  updateOwnerType: (owner: IOwner, type: OwnerType) => void;

  /**
   * A list of available owner types retrieved from the api
   * @type {Array<string>}
   * @memberof DatasetAuthor
   */
  ownerTypes: Array<string>;

  /**
   * Compares the source attribute on an owner, if it matches the OwnerSource.Ui type
   * @type {boolean}
   * @memberof DatasetAuthor
   */
  @equal('owner.owner.source', OwnerSource.Ui)
  isOwnerMutable: boolean;

  /**
   * Negates the owner attribute flag `isActive`, indicating owner record is considered inactive
   * @type {boolean}
   * @memberOf DatasetAuthor
   */
  @className('dataset-author-record--inactive')
  @not('owner.owner.isActive')
  isOwnerInActive: boolean;

  /**
   * Determines if the owner record is a system suggested owner and if this record is confirmed by a user
   * @type {boolean}
   * @memberof DatasetAuthor
   */
  @className('dataset-author-record--disabled')
  @computed('commonOwners')
  get isConfirmedSuggestedOwner(): boolean {
    const {
      commonOwners,
      isOwnerMutable,
      owner: {
        owner: { userName }
      }
    } = this;

    return isOwnerMutable ? false : !!commonOwners.findBy('userName', userName);
  }

  /**
   * Invokes the external action removeOwner to remove an owner from the confirmed list
   * @return {boolean | void | IOwner}
   */
  @action
  onRemoveOwner(): boolean | void | IOwner {
    const { owner, isOwnerMutable, removeOwner } = this;
    return isOwnerMutable && removeOwner(owner.owner);
  }

  /**
   * Invokes the external action for  confirming the suggested owner
   * @return {Array<IOwner> | void}
   */
  @action
  confirmOwner(): Array<IOwner> | void {
    const { owner, confirmSuggestedOwner } = this;
    return confirmSuggestedOwner(owner.owner);
  }

  /**
   * Updates the type attribute on the owner record
   * @param {OwnerType} type value to update the type attribute with
   * @return {void}
   */
  @action
  changeOwnerType(type: OwnerType): boolean | void {
    const { owner, isOwnerMutable, updateOwnerType } = this;

    return isOwnerMutable && updateOwnerType(owner.owner, type);
  }
}
