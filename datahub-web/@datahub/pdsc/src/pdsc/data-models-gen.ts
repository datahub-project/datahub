/**
 * Generate the .d.ts file describing the API types from their PDSC.
 */
import * as fs from 'fs';
import * as glob from 'glob';
import * as json5 from 'json5';
import * as path from 'path';
import { buildIgnoreRegExp, packageToNamespace } from '../ts-emitter';
import { generateTs, Schema, SchemaOptions } from './schema';

type Registry = Map<string, string[]>;

/**
 * Check to see if the `data` array starts with all the elements in `prefix`.
 *
 * @param data
 * @param prefix
 */
function startsWith(data: string[], prefix: string[]) {
  if (data.length < prefix.length) {
    return false;
  }
  for (let i = 0; i < prefix.length; i++) {
    if (data[i] !== prefix[i]) {
      return false;
    }
  }
  return true;
}

/**
 * Write the registry in one file, opening and closing namespaces as needed.
 *
 * Example
 *
 * declare namespace Foo {
 *   namespace Common {
 *     export type A = string;
 *   }
 *   namespace Other {
 *     export interface Baz {};
 *   }
 * }
 *
 * declare namespace Voyager {
 *   namespace Feed {
 *   }
 * }
 */
function serializeToFile(registry: Registry, outputFile: string, ignorePackages: RegExp[] | undefined): Promise<{}> {
  return new Promise((resolve, reject) => {
    const out = fs.createWriteStream(outputFile);
    const paths: string[] = [];
    out.on('finish', () => resolve());
    out.on('error', err => {
      const message = `error writing ${outputFile}: ${err}`;
      reject(message);
    });

    // Get a list of namespaces to group type definitions by.
    const namespaces: string[] = Array.from(registry.keys());
    namespaces.sort();

    // Process each namespace.
    for (const current of namespaces) {
      // Map from package to wanted namespaces list.
      const wanted = packageToNamespace(current, ignorePackages);

      // first close unwanted namespace
      while (paths.length > 0 && !startsWith(wanted, paths)) {
        out.write('}\n');
        paths.pop();
      }

      // For top level namespaces we need to output a `declare`.
      if (paths.length === 0) {
        out.write('declare ');
      }

      // Open desired namespaces.
      while (paths.length < wanted.length) {
        const component = wanted[paths.length];
        out.write(`namespace ${component} {\n`);
        paths.push(component);
      }

      // Output all the entries from the registry.
      const entries = registry.get(current);
      if (entries) {
        entries.forEach(entry => {
          out.write(entry);
        });
      }
    }
    // close all remaining namespaces`
    while (paths.length > 0 && paths.length) {
      out.write('}\n');
      paths.pop();
    }
    out.end();
  });
}

export interface DataModelsOptions {
  outputFile: string;
  ignorePackage?: string[];
  addType?: string;
}

/**
 * Generate the type information for the data models.
 *
 * @param inFolder folder containing pdsc files.
 * @param config
 */
export async function generateDataModels(inFolder: string, options: DataModelsOptions) {
  const registry: Map<string, string[]> = new Map();
  const ignoreRegExp = buildIgnoreRegExp(options.ignorePackage);
  const schemaOptions: SchemaOptions = {
    addType: options.addType,
    ignorePackages: ignoreRegExp
  };
  for (const file of glob.sync('**/*.pdsc', { cwd: inFolder })) {
    try {
      const fullPath = path.join(inFolder, file);
      const data = fs.readFileSync(fullPath).toString();
      const schema: Schema = json5.parse(data);
      const generated = generateTs(schema, file, schemaOptions);
      const documented = `// Generated from: ${file}\n\n${generated.value}`;

      const existing = registry.get(generated.namespace) || [];
      existing.push(documented);
      registry.set(generated.namespace, existing);
    } catch (e) {
      throw new Error(`Error processing '${file}': ${e}`);
    }
  }
  return serializeToFile(registry, options.outputFile, ignoreRegExp);
}
