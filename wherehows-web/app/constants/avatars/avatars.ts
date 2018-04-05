import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { avatar } from 'wherehows-web/constants/application';
import { pick } from 'lodash';

/**
 * Defines a partial interface for an object that can be narrowed into an avatar
 */
type PartAvatar = Partial<IAvatar>;

const { url: avatarUrl } = avatar;

/**
 * Default image url if an avatar url cannot be determined
 * @type {string}
 */
const defaultAvatarImage = '/assets/assets/images/default_avatar.png';

/**
 * Takes a PartAvatar object and build an object confirming to an IAvatar
 * @param {PartAvatar} object
 * @return {IAvatar}
 */
const getAvatarProps = (object: PartAvatar): IAvatar => {
  const props = pick(object, ['email', 'userName', 'name']);
  const imageUrl = props.userName ? avatarUrl.replace('[username]', props.userName) : defaultAvatarImage;

  return {
    imageUrl,
    ...props
  };
};

export { getAvatarProps };
