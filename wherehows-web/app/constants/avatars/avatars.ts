import { IAvatar } from 'wherehows-web/typings/app/avatars';
import { pick } from 'lodash';
import { IAppConfig } from 'wherehows-web/typings/api/configurator/configurator';

/**
 * Takes a Partial<IAvatar> object and builds an IAvatar
 * @param {Partial<IAvatar>} object
 * @param {IAppConfig.userEntityProps.aviUrlPrimary} aviUrlPrimary primary url for avatar image
 * @param {IAppConfig.userEntityProps.aviUrlFallback} aviUrlFallback
 * @return {IAvatar}
 */
const makeAvatar = ({ aviUrlPrimary, aviUrlFallback = '' }: IAppConfig['userEntityProps']) => (
  object: Partial<IAvatar>
): IAvatar => {
  const props = pick(object, ['email', 'userName', 'name']);
  const hasRequiredUrlElements = props.userName && aviUrlPrimary;

  return {
    imageUrl: hasRequiredUrlElements ? aviUrlPrimary.replace('[username]', props.userName!) : aviUrlFallback,
    imageUrlFallback: aviUrlFallback,
    ...props
  };
};

export { makeAvatar };
