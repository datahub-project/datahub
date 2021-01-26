import { createIndexes, loopDependencies } from '../utils';
import { IPackageJson } from '../../types/index';

/**
 * Will create 'transitive' dependencies which are the ones that this package needs to build itself
 * but not required for other packages.
 */
export default async function createTransitiveDependencies(): Promise<void> {
  const jsons = await createIndexes();

  await loopDependencies(
    (json: IPackageJson): Promise<IPackageJson> => {
      let transitiveDependencies = {};
      const allDeps = { ...json.dependencies, ...json.devDependencies };
      Object.keys(allDeps).forEach(key => {
        const json = jsons[key];
        if (json && json.peerDependencies) {
          transitiveDependencies = { ...transitiveDependencies, ...json.peerDependencies };
        }
      });
      return Promise.resolve({ ...json, transitiveDependencies });
    }
  );
}
