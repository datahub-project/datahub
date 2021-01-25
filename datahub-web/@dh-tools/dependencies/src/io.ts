import { join } from 'path';
import { readdirSync, writeFileSync, statSync, readFileSync } from 'fs';
import { IPackageJson } from '../types/index';

/**
 * Cached json paths
 */
let cachedPackageJsonPaths: Array<string>;

/**
 * Will explore all folder to return all the package.json files
 */
export const returnPackageJsons = async function(): Promise<Array<string>> {
  if (!cachedPackageJsonPaths) {
    const explorationPaths = [join(__dirname, '../../../')];
    cachedPackageJsonPaths = [];
    while (explorationPaths.length !== 0) {
      const currentExploration = explorationPaths.pop();
      const files = await readdirSync(currentExploration);
      for (const fileIndex in files) {
        const file = files[fileIndex];
        const filePath = join(currentExploration, file);
        const stat = await statSync(filePath);
        if (
          stat &&
          stat.isDirectory() &&
          !file.endsWith('tmp') &&
          !file.endsWith('dist') &&
          !file.endsWith('build') &&
          !file.endsWith('node_modules') &&
          !file.startsWith('.')
        ) {
          explorationPaths.push(filePath);
        } else if (file.endsWith('package.json')) {
          cachedPackageJsonPaths.push(filePath);
        }
      }
    }
  }
  return cachedPackageJsonPaths;
};

/**
 * Will read a json
 * @param jsonPath
 */
export const readJson = async function(jsonPath: string): Promise<IPackageJson> {
  const rawdata = await readFileSync(jsonPath);
  return JSON.parse(rawdata.toString());
};

/**
 * Will write a json
 * @param jsonPath
 * @param json
 */
export const writeJson = async function(jsonPath: string, json: IPackageJson): Promise<void> {
  await writeFileSync(jsonPath, JSON.stringify(json, null, 2) + '\n');
};
