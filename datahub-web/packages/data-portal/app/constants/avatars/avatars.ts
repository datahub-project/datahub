import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { pick } from 'lodash';
import { IAppConfig } from '@datahub/shared/types/configurator/configurator';

type AvatarCreatorFunc = (obj: Partial<IAvatar>) => IAvatar;

// gray circle
const fallback = 'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7';

/**
 * Takes a Partial<IAvatar> object and builds an IAvatar
 * @param {Partial<IAvatar>} object
 * @param {IAppConfig.userEntityProps.aviUrlPrimary} aviUrlPrimary primary url for avatar image
 * @param {IAppConfig.userEntityProps.aviUrlFallback} aviUrlFallback
 * @return {IAvatar}
 */
const makeAvatar = ({ aviUrlPrimary, aviUrlFallback = fallback }: IAppConfig['userEntityProps']): AvatarCreatorFunc => (
  object: Partial<IAvatar>
): IAvatar => {
  const props = pick(object, ['email', 'userName', 'name', 'imageUrl', 'pictureLink']);
  const { userName, pictureLink } = props;
  const imageUrlFallback = aviUrlFallback || fallback;
  const imageUrl = pictureLink
    ? pictureLink
    : userName && aviUrlPrimary
    ? aviUrlPrimary.replace('[username]', userName)
    : imageUrlFallback;

  return {
    imageUrlFallback,
    imageUrl,
    ...props
  };
};

export { makeAvatar };
