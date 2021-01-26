import { IPackageJson } from '../../types';
import { loopDependencies, sortDeps } from '../utils';

/**
 * Will return all the dependencies in the project across all modules
 */
export default async function getAllDeps(): Promise<Record<string, Array<string>>> {
  const allDeps: Record<string, Array<string>> = {};
  await loopDependencies(
    (json: IPackageJson): Promise<IPackageJson> => {
      const deps = { ...json.devDependencies, ...json.dependencies };

      Object.keys(deps)
        .sort()
        .forEach(depKey => {
          if (!allDeps[depKey]) {
            allDeps[depKey] = [];
          }
          const array = allDeps[depKey];
          if (array.indexOf(deps[depKey]) < 0) {
            allDeps[depKey].push(deps[depKey]);
          }
        });
      return Promise.resolve(json);
    }
  );

  return sortDeps(allDeps);
}
