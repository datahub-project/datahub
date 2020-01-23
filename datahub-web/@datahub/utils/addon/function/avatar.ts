import { IAvatar, IUserEntityProps } from '@datahub/utils/types/avatars';
import { pick } from 'lodash';

// Aliases the function interface returned from the invocation of makeAvatar
type AvatarCreatorFunc = (obj: Partial<IAvatar>) => IAvatar;

/**
 * Takes a Partial<IAvatar> object and builds an IAvatar
 * @param {Partial<IAvatar>} object
 * @param {IAppConfig.userEntityProps.aviUrlPrimary} aviUrlPrimary primary url for avatar image
 * @param {IAppConfig.userEntityProps.aviUrlFallback} aviUrlFallback if the primary url fails, uses the provided fallback url to fetch the avatar
 */
export const makeAvatar = ({ aviUrlPrimary, aviUrlFallback = '' }: IUserEntityProps): AvatarCreatorFunc => (
  object: Partial<IAvatar>
): IAvatar => {
  const props = pick(object, ['email', 'userName', 'name']);
  const { userName } = props;

  return {
    imageUrl: userName && aviUrlPrimary ? aviUrlPrimary.replace('[username]', () => userName) : aviUrlFallback,
    imageUrlFallback: aviUrlFallback,
    ...props
  };
};
