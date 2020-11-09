import { IPackageJson } from '../../types';
import { loopDependencies } from '../utils';

/**
 * Will merge peer and other dependencies into Dev so they can be loaded as peer dependencies are not automatically
 * loaded when running yarn
 */
export default function mergeDev(): Promise<void> {
  return loopDependencies(
    (json: IPackageJson): Promise<IPackageJson> => {
      return Promise.resolve({
        ...json,
        devDependencies: {
          ...json.devDependencies,
          ...json.peerDependencies,
          ...json.transitiveDependencies
        }
      });
    }
  );
}
