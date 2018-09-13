import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { pick } from 'lodash';
import { IAppConfig } from 'wherehows-web/typings/api/configurator/configurator';

/**
 * Takes a Partial<IAvatar> object and builds an IAvatar
 * @param {Partial<IAvatar>} object
 * @param {IAppConfig.avatarEntityProps.urlPrimary} urlPrimary
 * @param {IAppConfig.avatarEntityProps.urlPrimary} urlFallback
 * @return {IAvatar}
 */
const getAvatarProps = ({ urlPrimary, urlFallback }: IAppConfig['avatarEntityProps']) => (
  object: Partial<IAvatar>
): IAvatar => {
  const props = pick(object, ['email', 'userName', 'name']);
  let imageUrl = urlFallback || '';

  if (props.userName && urlPrimary) {
    imageUrl = urlPrimary.replace('[username]', props.userName);
  }

  return {
    imageUrl,
    imageUrlFallback: urlFallback,
    ...props
  };
};

export { getAvatarProps };
