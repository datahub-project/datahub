import * as fs from 'fs';
import * as glob from 'glob';
import * as json5 from 'json5';
import * as mkdirp from 'mkdirp';
import * as path from 'path';

import { getBinary, getContent, unzip } from '../download-cache';
import * as ts from '../ts-emitter';
import { RestSpec } from './rest-spec';

const CACHE_BUCKET = 'pi-restli-types';
export interface Options {
  resourceFilter?: string[];
  cacheFolder: string;
}

/**
 * Load the rest spec content from the file.
 *
 * @param fullPath
 */
export function loadRestSpec(fullPath: string): RestSpec {
  const data = fs.readFileSync(fullPath).toString();
  const restSpec: RestSpec = json5.parse(data);
  return restSpec;
}

/**
 * Process a local idl file.
 *
 * @param file zip/jar file containing restspec.json files.
 * @param options
 */
export function processIdlFolder(inFolder: string, options: Options): RestSpec[] {
  const out: RestSpec[] = [];
  for (const file of glob.sync('**/*.restspec.json', { cwd: inFolder })) {
    try {
      const fullPath = path.join(inFolder, file);
      out.push(loadRestSpec(fullPath));
    } catch (err) {
      throw new Error(`Error parsing ${file}: ${err}`);
    }
  }
  return out;
}

export function generateTs(restSpec: RestSpec, options: { ignoreNamespace: string; collectionType: string }): string {
  const parts: string[] = [];

  parts.push(ts.formatDocs(restSpec.doc));

  if (restSpec.schema) {
    const entityType = restSpec.schema.startsWith(options.ignoreNamespace)
      ? restSpec.schema.substr(options.ignoreNamespace.length)
      : restSpec.schema;
    const capitalized = entityType
      .split('.')
      .map(ts.capitalize)
      .join('.');
    parts.push(`\nexport type Entity = ${capitalized};`);
    parts.push(`export type Result = ${options.collectionType}<Entity>;`);
  }

  parts.push(`\nexport const resourceUrl = '${ts.makeRelative(restSpec.path)}';`);

  if (restSpec.collection) {
    if (restSpec.collection.finders && restSpec.collection.finders.length > 0) {
      parts.push(`\nexport const Finder = {`);
      restSpec.collection.finders.forEach(finder => {
        parts.push('');
        if (finder.doc) {
          parts.push(ts.formatDocs(finder.doc, '  '));
        }

        parts.push(`  ${finder.name}: {`);
        parts.push(`    name: '${finder.name}',`);
        if (finder.parameters) {
          parts.push(`    parameters: [`);
          finder.parameters.forEach(p => {
            parts.push('      {');
            if (p.doc) {
              parts.push(`        doc: ${ts.stringConstant(p.doc)},`);
            }
            parts.push(`        name: '${p.name}',`);
            parts.push(`        type: '${p.type}',`);
            parts.push('      },');
          });
          parts.push('    ],');
        }
        parts.push('  },');
      });
      parts.push('};');
    }
  }
  return parts.join('\n') + '\n';
}
