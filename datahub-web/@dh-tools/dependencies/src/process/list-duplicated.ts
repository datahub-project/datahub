import { IPackageJson } from '../../types';
import { loopDependencies } from '../utils';

/**
 * Will return all the dependencies in the project across all modules
 */
export default async function getAllDeps(): Promise<void> {
  const allDeps: Record<string, number> = {};
  await loopDependencies(
    (json: IPackageJson): Promise<IPackageJson> => {
      const deps = { ...json.dependencies };

      Object.keys(deps)
        .sort()
        .forEach(depKey => {
          if (!allDeps[depKey]) {
            allDeps[depKey] = 0;
          }
          allDeps[depKey] += 1;
        });
      return Promise.resolve(json);
    }
  );
  // eslint-disable-next-line no-console
  console.log(
    Object.keys(allDeps)
      .map(key => `${String(allDeps[key]).padStart(2, '0')} - ${key}`)
      .sort()
  );
}
