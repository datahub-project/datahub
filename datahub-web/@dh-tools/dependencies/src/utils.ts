import { IPackageJson } from '../types';
import { readJson, returnPackageJsons, writeJson } from './io';

/**
 * Will sort dependencies and exclude some dependencies if needed
 * @param obj
 * @param excludeKeys
 */
export const sortDeps = function<T>(obj: Record<string, T>, excludeKeys: Record<string, T> = {}): Record<string, T> {
  const ordered = {};
  Object.keys(obj)
    .sort()
    .forEach(key => {
      if (!excludeKeys[key]) {
        ordered[key] = obj[key];
      }
    });
  return ordered;
};

/**
 * Will loop through all package.json performing an operation
 * @param operation
 */
export const loopDependencies = async function(
  operation: (json: IPackageJson, jsonPath: string) => Promise<IPackageJson>
): Promise<void> {
  const jsons = await returnPackageJsons();
  for (const jsonIndex in jsons) {
    const jsonPath = jsons[jsonIndex];
    const json = await readJson(jsonPath);
    const newJson = await operation(json, jsonPath);
    await writeJson(jsonPath, {
      ...newJson,
      dependencies: newJson.dependencies ? sortDeps(newJson.dependencies) : undefined,
      devDependencies: newJson.devDependencies ? sortDeps(newJson.devDependencies, newJson.dependencies) : undefined,
      peerDependencies: newJson.peerDependencies ? sortDeps(newJson.peerDependencies) : undefined,
      transitiveDependencies: newJson.transitiveDependencies ? sortDeps(newJson.transitiveDependencies) : undefined
    });
  }
};

/**
 * Will create an index for file name and package.json
 */
export const createIndexes = async (): Promise<Record<string, IPackageJson>> => {
  const index = {};
  await loopDependencies(
    (json: IPackageJson): Promise<IPackageJson> => {
      index[json.name] = json;
      return Promise.resolve(json);
    }
  );
  return index;
};

/**
 * Will return data-portal json
 */
export const findMainJsonPath = async (): Promise<string> => {
  let dataPortalJsonPath;
  await loopDependencies(
    (json: IPackageJson, jsonPath: string): Promise<IPackageJson> => {
      if (jsonPath.indexOf('data-portal/package.json') >= 0) {
        dataPortalJsonPath = jsonPath;
      }
      return Promise.resolve(json);
    }
  );
  return dataPortalJsonPath;
};

/**
 * Removes a specific key from a record
 * @param record
 * @param keyToFilter
 */
export const filterRecord = <T>(record: Record<string, T>, keyToFilter: string): Record<string, T> => {
  return Object.keys(record)
    .filter(key => key !== keyToFilter)
    .reduce((newRecord, key) => ({ ...newRecord, [key]: record[key] }), {});
};
