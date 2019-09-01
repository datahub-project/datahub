import { IOwner } from 'wherehows-web/typings/api/datasets/owners';
import { IAvatar } from 'wherehows-web/typings/app/avatars';

/**
 * An IOwner instance augmented with an IAvatar Record keyed by 'avatar'
 * @type OwnerWithAvatarRecord
 * @alias
 */
export type OwnerWithAvatarRecord = Record<'owner', IOwner> & Record<'avatar', IAvatar> & Record<'profile', string>;
