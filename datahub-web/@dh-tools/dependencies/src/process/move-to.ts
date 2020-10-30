import { IPackageJson } from '../../types';
import { loopDependencies, filterRecord } from '../utils';

/**
 * Will fix auto import location as it should live under dependencies and not dev dependencies
 */
export default function fixAutoImportLocation(): Promise<void> {
  const myArgs = process.argv.slice(3);
  const [packageName, location] = myArgs;
  return loopDependencies(
    (json: IPackageJson): Promise<IPackageJson> => {
      const allDeps = { ...json.dependencies, ...json.devDependencies };
      const dependency = allDeps[packageName];
      if (dependency) {
        const cleanDeps = filterRecord(json.dependencies, packageName);
        const cleanDevDeps = filterRecord(json.devDependencies, packageName);
        const newKey = { [packageName]: dependency };
        const newDependency = location !== 'dev' ? newKey : {};
        const newDevDependency = location === 'dev' ? newKey : {};
        return Promise.resolve({
          ...json,
          dependencies: { ...cleanDeps, ...newDependency },
          devDependencies: { ...cleanDevDeps, ...newDevDependency }
        });
      }
      return Promise.resolve(json);
    }
  );
}
