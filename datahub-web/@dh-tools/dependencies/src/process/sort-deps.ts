import { IPackageJson } from '../../types';
import { loopDependencies } from '../utils';

/**
 * Will read and save dependencies. This will automatically sort and deduplicate them
 */
export default function sortDependencies(): Promise<void> {
  return loopDependencies(
    (json: IPackageJson): Promise<IPackageJson> => {
      return Promise.resolve(json);
    }
  );
}
