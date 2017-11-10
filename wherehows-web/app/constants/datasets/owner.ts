import { IOwner } from 'wherehows-web/typings/api/datasets/owners';
import { OwnerIdType, OwnerSource, OwnerType, OwnerUrnNamespace } from 'wherehows-web/utils/api/datasets/owners';
import { isListUnique } from 'wherehows-web/utils/array';

/**
 * Initial user name for candidate owners
 * @type {string}
 */
const defaultOwnerUserName = 'New Owner';

/**
 * The minimum required number of owners
 * @type {number}
 */
const minRequiredConfirmedOwners = 2;

/**
 * Class to toggle readonly mode vs edit mode
 * @type {string}
 */
const userNameEditableClass = 'dataset-author-cell--editing';

/**
 * Checks that a userName already exists in the list of IOwner instances
 * @param {Array<IOwner>} owners the list of owners
 * @param {Pick<IOwner, 'userName'>} newOwner userName for the owner
 * @returns {boolean} true if owner username in current list of owners
 */
const ownerAlreadyExists = (owners: Array<IOwner>, newOwner: Pick<IOwner, 'userName'>) => {
  const newUserNameRegEx = new RegExp(`.*${newOwner.userName}.*`, 'i');

  return owners.mapBy('userName').some((userName: string) => newUserNameRegEx.test(userName));
};

// overloads
function updateOwner(owners: Array<IOwner>, owner: IOwner, props: IOwner): void | Array<IOwner>;
function updateOwner<K extends keyof IOwner>(
  owners: Array<IOwner>,
  owner: IOwner,
  props: K,
  value: IOwner[K]
): void | Array<IOwner>;
/**
 * Updates an IOwner instance in a list of IOwners using a known key and expected value type,
 * or a replacement attributes
 * @template K
 * @param {Array<IOwner>} owners the list containing the owners
 * @param {IOwner} owner the owner to update
 * @param {(K | IOwner)} props the properties to replace the IOwner instance with, or a singe IOwner attribute
 * @param {IOwner[K]} [value] optional value to update the attribute with
 * @returns {(void | Array<IOwner>)} the updated list of owners if the owner list contains no duplicates
 */
function updateOwner<K extends keyof IOwner>(
  owners: Array<IOwner>,
  owner: IOwner,
  props: K | IOwner,
  value?: IOwner[K]
): void | Array<IOwner> {
  // creates a local working copy of the list of owners
  const updatingOwners = [...owners];

  // ensure that the owner is in the list by referential equality
  if (updatingOwners.includes(owner)) {
    const ownerPosition = updatingOwners.indexOf(owner);
    let updatedOwner: IOwner;

    // if props is a string, i.e. attribute IOwner, override the previous value,
    // otherwise replace with new attributes
    if (typeof props === 'string') {
      updatedOwner = { ...owner, [props]: value };
    } else {
      updatedOwner = props;
    }

    // retain update position
    const updatedOwners: Array<IOwner> = [
      ...updatingOwners.slice(0, ownerPosition),
      updatedOwner,
      ...updatingOwners.slice(ownerPosition + 1)
    ];

    // each owner is uniquely identified by the composite key of userName and source
    const userKeys = updatedOwners.map(({ userName, source }) => `${userName}:${source}`);

    // ensure we have not duplicates
    if (isListUnique(userKeys)) {
      return owners.setObjects(updatedOwners);
    }
  }
}

/**
 * Sets the `confirmedBy` attribute to the currently logged in user
 * @param {Array<IOwner>} owners the list of owners
 * @param {IOwner} owner the owner to be updated
 * @param {string} confirmedBy the userName of the confirming user
 * @returns {(Array<IOwner> | void)}
 */
const confirmOwner = (owners: Array<IOwner>, owner: IOwner, confirmedBy: string): Array<IOwner> | void => {
  const isConfirmedBy = confirmedBy || null;
  return updateOwner(owners, owner, 'confirmedBy', isConfirmedBy);
  // return set(owner, 'confirmedBy', isConfirmedBy);
};

/**
 * Defines the default properties for a newly created IOwner instance
 *@type {IOwner}
 */
const defaultOwnerProps: IOwner = {
  userName: defaultOwnerUserName,
  email: null,
  name: '',
  isGroup: false,
  namespace: OwnerUrnNamespace.groupUser,
  type: OwnerType.Owner,
  subType: null,
  sortId: 0,
  source: OwnerSource.Ui,
  confirmedBy: null,
  idType: OwnerIdType.User
};

export {
  defaultOwnerProps,
  defaultOwnerUserName,
  minRequiredConfirmedOwners,
  userNameEditableClass,
  ownerAlreadyExists,
  updateOwner,
  confirmOwner
};
