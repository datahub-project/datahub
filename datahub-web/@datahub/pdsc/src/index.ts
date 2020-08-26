// tslint:disable:no-console no-string-literal
import * as fs from 'fs';
import * as minimist from 'minimist';
import * as mkdirp from 'mkdirp';
import * as path from 'path';
import { format as pretty, Options as PrettyOptions } from 'prettier';

import { Config } from './config';
import { DataModelsOptions, generateDataModels } from './pdsc/data-models-gen';
import { generateTs, processIdlFolder } from './rest-spec/api-declaration-gen';

const CONFIG = 'restli-types-config.json';

export * from './ts-emitter';

/**
 * Options to use when prettier-ing
 */
const PRETTY_OPTIONS: PrettyOptions = {
  parser: 'typescript',
  singleQuote: true
};

/**
 * Generate the interface definitions (url name and structure).
 *
 * @param idlArchive filename
 * @param config
 */
export async function generateIdl(idlArchive: string, config: Config) {
  const restModels = await processIdlFolder(idlArchive, {
    cacheFolder: config.cacheFolder
  });

  if (restModels.length === 0) {
    throw new Error(`Did not generate any models from idl ${idlArchive}`);
  }

  // Output the resources under the resources folder.
  const resources = path.join(config.output, 'resources');
  mkdirp.sync(resources);

  restModels.forEach(rm => {
    const filename = path.join(resources, `${rm.name}.ts`);
    try {
      const content = generateTs(rm, {
        collectionType: config.collectionType,
        ignoreNamespace: 'com.'
      });
      const formatted = pretty(content, PRETTY_OPTIONS);
      fs.writeFileSync(filename, formatted);
    } catch (err) {
      const augmented = `Error on ${filename}: ${err}`;
      throw new Error(augmented);
    }
  });
}

export async function codegen(
  idlArchive: string | undefined,
  dataModel: string | undefined,
  ignorePackage: string[],
  addType: string | undefined
) {
  const config = loadConfig();
  if (!dataModel) {
    throw new Error(`Missing required cli argument data-model`);
  }

  mkdirp.sync(config.output);
  if (idlArchive && fs.existsSync(idlArchive)) {
    try {
      generateIdl(idlArchive, config);
    } catch (err) {
      throw new Error(`Error generating idl: ${err}`);
    }
  }

  const outputFile = path.join(config.output, 'index.d.ts');
  try {
    if (fs.existsSync(outputFile)) {
      fs.unlinkSync(outputFile);
    }
    const options: DataModelsOptions = {
      addType,
      ignorePackage,
      outputFile
    };
    await generateDataModels(dataModel, options);
    const formatted = pretty(fs.readFileSync(outputFile).toString(), PRETTY_OPTIONS);
    fs.writeFileSync(outputFile, formatted);
  } catch (err) {
    // Save the file for easier debugging
    if (fs.existsSync(outputFile)) {
      const errorFile = path.join(config.output, 'index.err.d.ts');
      fs.renameSync(outputFile, errorFile);
    }
    // Log the error here since the stack trace gets lost.
    console.error('Error in generating and formatting data models', err);

    throw new Error(`Error generating data models in ${dataModel}: ${err}`);
  }
}

function loadConfig(): Config {
  let config: Config;
  if (fs.existsSync(CONFIG)) {
    config = JSON.parse(fs.readFileSync(CONFIG).toString());
    // backwards compatibility with Sailfish where this library originated
    if (!config.collectionType) {
      config.collectionType = 'Sailfish.Restli.Collection';
    }
  } else {
    throw new Error(`Missing configuration file ${CONFIG}`);
  }
  return config;
}

if (!module.parent) {
  /**
   * Main command line entry point.
   * @param args
   */
  async function main(args: string[]) {
    const cli = minimist(process.argv.slice(2));

    const idlArchive: string | undefined = cli['rest-spec'];
    const dataModel: string | undefined = cli['data-model'];
    const ignorePackageOpt: string | string[] | undefined = cli['ignore-package'];
    const addType: string | undefined = cli['add-type'];
    let ignorePackage: string[] = [];
    if (ignorePackageOpt) {
      ignorePackage = typeof ignorePackageOpt === 'string' ? [ignorePackageOpt] : ignorePackageOpt;
    }
    codegen(idlArchive, dataModel, ignorePackage, addType);
  }

  main(process.argv)
    .then(() => console.log('pi-restli-types completed successfully'))
    .catch(err => {
      console.error('pi-restli-types failed with:\n', err);
    });
}
