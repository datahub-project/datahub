import { set } from '@ember/object';
import { IOwner } from 'datahub-web/typings/api/datasets/owners';
import { OwnerIdType, OwnerSource, OwnerType, OwnerUrnNamespace } from 'datahub-web/utils/api/datasets/owners';
import { arrayFilter, isListUnique } from '@datahub/utils/array/index';
import { IAvatar } from 'datahub-web/typings/app/avatars';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

/**
 * Initial user name for candidate owners
 * @type {string}
 */
export const defaultOwnerUserName = 'New Owner';

/**
 * The minimum required number of owners with a confirmed status
 * @type {number}
 */
export const minRequiredConfirmedOwners = 2;

/**
 * Checks that a userName & source pair already exists in the list of IOwner instances
 * @param {Array<IOwner>} owners the list of owners
 * @param {string} userName userName to check for uniqueness
 * @param {OwnerSource} source source to include in composite unique key
 * @return {boolean} true if owner username in current list of owners
 */
export const ownerAlreadyExists = (
  owners: Array<IOwner>,
  { userName, source }: Pick<IOwner, 'userName' | 'source'>
): boolean =>
  userName && source
    ? owners.map(({ userName, source }) => `${userName}:${source}`).includes(`${userName}:${source}`)
    : false;

// overloads
export function updateOwner(owners: Array<IOwner>, owner: IOwner, props: IOwner): void | Array<IOwner>;
export function updateOwner<K extends keyof IOwner>(
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
export function updateOwner<K extends keyof IOwner>(
  owners: Array<IOwner> = [],
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
 * @param {IOwner} owner the owner to be updated
 * @param {string} confirmedBy the userName of the confirming user
 * @returns {IOwner.confirmedBy}
 */
export const confirmOwner = (owner: IOwner, confirmedBy: string): IOwner['confirmedBy'] =>
  set(owner, 'confirmedBy', confirmedBy || null);

/**
 * Defines the default properties for a newly created IOwner instance
 *@type {IOwner}
 */
export const defaultOwnerProps: IOwner = {
  userName: defaultOwnerUserName,
  email: null,
  name: '',
  isGroup: false,
  namespace: OwnerUrnNamespace.corpUser,
  type: OwnerType.Owner,
  subType: null,
  sortId: 0,
  source: OwnerSource.Ui,
  confirmedBy: null,
  idType: OwnerIdType.User,
  isActive: true
};

/**
 * Given an IOwner object, determines if it qualifies as a valid confirmed owner
 * @param {IOwner}
 * @return {boolean}
 */
export const isValidConfirmedOwner = ({ confirmedBy, type, idType, isActive }: IOwner): boolean =>
  !!confirmedBy && type === OwnerType.Owner && idType === OwnerIdType.User && isActive;

/**
 * Filters out a list of valid confirmed owners in a list of owners
 * @type {(array: Array<IOwner> = []) => Array<IOwner>}
 */
export const validConfirmedOwners = arrayFilter(isValidConfirmedOwner);

/**
 * Checks if an owner has been confirmed by a user, i.e. OwnerSource.Ui
 * @param {IOwner} { source }
 * @returns {boolean}
 */
export const isConfirmedOwner = ({ source }: IOwner): boolean => source === OwnerSource.Ui;

/**
 * Takes a list of owners and returns those that are confirmed
 * @type {(array: Array<IOwner>) => Array<IOwner>}
 */
export const confirmedOwners = arrayFilter(isConfirmedOwner);

/**
 * Checks that the required minimum number of confirmed users is met with the type Owner and idType User
 * @param {Array<IOwner>} owners the list of owners to check
 * @return {boolean}
 */
export const isRequiredMinOwnersNotConfirmed = (owners: Array<IOwner> = []): boolean =>
  validConfirmedOwners(owners).length < minRequiredConfirmedOwners;

/**
 * Augments an owner instance of IAvatar requiring property avatarOptions to exist on the returned instance
 * @param {IAvatar} avatar the avatar object to augment
 * @returns {(IAvatar & Required<Pick<IAvatar, 'avatarOptions'>>)}
 */
export const avatarWithDropDownOption = (avatar: IAvatar): IAvatar & Required<Pick<IAvatar, 'avatarOptions'>> => {
  const email = avatar.email || '';

  return {
    ...avatar,
    avatarOptions: [
      {
        value: email,
        label: email
      }
    ]
  };
};

/**
 * Augments an owner instance of IAvatar by providing the profile link
 * @param avatar the avatar object to augment
 */
export const avatarWithProfileLink = (avatar: IAvatar): IAvatar => ({
  ...avatar,
  profileLink: PersonEntity.profileLinkFromUsername(avatar.userName || '')
});
