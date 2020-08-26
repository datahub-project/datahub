import { IAvatar } from 'datahub-web/typings/app/avatars';
import { pick } from 'lodash';
import { IAppConfig } from '@datahub/shared/types/configurator/configurator';

type AvatarCreatorFunc = (obj: Partial<IAvatar>) => IAvatar;

/**
 * Takes a Partial<IAvatar> object and builds an IAvatar
 * @param {Partial<IAvatar>} object
 * @param {IAppConfig.userEntityProps.aviUrlPrimary} aviUrlPrimary primary url for avatar image
 * @param {IAppConfig.userEntityProps.aviUrlFallback} aviUrlFallback
 * @return {IAvatar}
 */
const makeAvatar = ({ aviUrlPrimary, aviUrlFallback = '' }: IAppConfig['userEntityProps']): AvatarCreatorFunc => (
  object: Partial<IAvatar>
): IAvatar => {
  const props = pick(object, ['email', 'userName', 'name']);
  const { userName } = props;

  return {
    imageUrl: userName && aviUrlPrimary ? aviUrlPrimary.replace('[username]', userName) : aviUrlFallback,
    imageUrlFallback: aviUrlFallback,
    ...props
  };
};

export { makeAvatar };
