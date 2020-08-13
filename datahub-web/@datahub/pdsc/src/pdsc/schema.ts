import * as path from 'path';

import { packageToNamespace, typeToNamespace } from '../ts-emitter';
import { generate, SimpleField } from './field';

export type FieldType = SimpleField | string | string[] | Schema;

/**
 * A PDSC type can be complex or primitive. This discriminator picks between
 * the two.
 *
 * @param type
 */
function isComplexType(type: FieldType): type is Schema {
  return typeof type === 'object' && !!(type as Schema).type;
}

export interface Field {
  name: string;
  type: FieldType;
  doc?: string;
  optional?: boolean;
  default?: any;
  derived: boolean;
}

export interface PdscArray {
  type: 'array';
  name?: string;
  namespace?: string;
  doc?: string;
  items: FieldType;
}

export interface PdscEnum {
  type: 'enum';
  name?: string;
  namespace?: string;
  doc?: string;
  symbols: string[];
}

export interface PdscFixed {
  type: 'fixed';
  name?: string;
  namespace?: string;
  doc?: string;
  size: number;
}

export interface PdscMap {
  type: 'map';
  name?: string;
  namespace?: string;
  doc?: string;
  values: FieldType;
}

export interface PdscRecord {
  type: 'record';
  name?: string;
  namespace?: string;
  doc?: string;
  fields: Field[];
  include: string[];
}

export interface PdscTyperef {
  type: 'typeref';
  name?: string;
  namespace?: string;
  doc?: string;
  ref: FieldType;
}

export interface PdscSubrecord {
  alias?: string;
  ref: FieldType;
}

export interface PdscUnion {
  type: string[];
  name?: string;
  namespace?: string;
  doc?: string;
}

export type Schema = PdscArray | PdscEnum | PdscFixed | PdscMap | PdscRecord | PdscTyperef | PdscUnion;

/**
 * A generated typescript class.
 */
export class Generated {
  constructor(public name: string, public namespace: string, public value: string) {}
}

/**
 * Generate typescripts for the fields of a PDSC record.
 *
 * @param fields
 * @param options
 */
function generateRecordFields(fields: Field[], options: SchemaOptions): string {
  const body = fields.map(f => {
    const name = `${f.name}${f.optional ? '?' : ''}`;
    return `${name}: ${generateSchema(f.type, options)};`;
  });
  // Add the optional field type, if there are already fields.
  if (options.addType && fields.length) {
    body.push(`${options.addType}?: string`);
  }
  return `{\n  ${body.join('\n  ')}\n}`;
}

/**
 * Generate the typescript representation for the given type.GG
 *
 * @param type
 */
function generateSchema(type: FieldType, options: SchemaOptions): string {
  const generator = (t: FieldType) => generateSchema(t, options);
  if (Array.isArray(type)) {
    if (type.length === 0) {
      return 'never';
    }
    return type.map(generator).join('\n | ');
  }
  if (isComplexType(type)) {
    const subType = type.type;
    switch (subType) {
      case 'array': {
        const items = (type as PdscArray).items;
        const translated = generator(items);
        if (typeof items === 'string') {
          return `${translated}[]`;
        } else {
          return `Array<${translated}>`;
        }
      }
      case 'enum': {
        let values = (type as PdscEnum).symbols;
        if (!values || !values.length) {
          values = ['NONE'];
        }
        return values.map(a => `'${a}'`).join('\n | ');
      }
      case 'map': {
        const values = (type as PdscMap).values;
        const translated = generator(values);
        return `{[id: string]: ${translated}}`;
      }
      case 'record': {
        const fields = (type as PdscRecord).fields;

        return `${generateRecordFields(fields, options)}`;
      }
      case 'typeref': {
        return generator((type as PdscTyperef).ref);
      }
      default: {
        if (isComplexType(subType)) {
          return generator(subType);
        }
        if (typeof subType === 'string') {
          const simple = generate(subType as SimpleField);
          if (simple) {
            return simple;
          }

          // we have a package name
          const fieldName = (type as PdscSubrecord).alias || `'${subType}'`;
          const value = typeToNamespace(subType, options.ignorePackages);
          return `{ ${fieldName}: ${value} }`;
        }
      }
    }
  }
  if (typeof type === 'string') {
    const simple = generate(type as SimpleField);
    if (simple) {
      return simple;
    }
    // we have a package name
    return typeToNamespace(type, options.ignorePackages);
  }
  throw new Error(`Cannot translate ${type}`);
}

/**
 * Generate a (namespaced ?) list of interfaces to extend, based on the include field of a PDSC record.
 *
 * @param includes
 * @param options
 */
function generateIncludeNames(includes: string[], options: SchemaOptions): string {
  if (!includes) {
    return '';
  }

  return includes
    .map(a =>
      packageToNamespace(a, options.ignorePackages)
        .filter(s => s) // swallow empty strings
        .join('.')
    )
    .join(', ');
}

export interface SchemaOptions {
  addType: string | undefined;
  ignorePackages: RegExp[] | undefined;
}

/**
 * Generate the typescript representation of a top level schema.
 *
 * @param schema top level schema
 * @param file file path for the current schema, must start at the package, for example "com/linkedin/.."
 */
export function generateTs(schema: Schema, file: string | undefined, options: SchemaOptions): Generated {
  const name = schema.name || (file && path.basename(file, '.pdsc'));
  if (!name) {
    throw new Error(`Missing name in ${schema}, file ${file}`);
  }
  const namespace = schema.namespace || (file && path.dirname(file).replace(/\//g, '.'));
  if (!namespace) {
    throw new Error(`Missing namespace in ${schema}, file ${file}`);
  }

  // if there are includes on this PdscRecord, generate a list of include names .. otherwise nothing.
  const extensions =
    (schema.type === 'record' &&
      (schema as PdscRecord).include &&
      `extends ${generateIncludeNames((schema as PdscRecord).include, options)} `) ||
    ``;

  const value =
    schema.type === 'record'
      ? `export interface ${name} ${extensions}${generateRecordFields((schema as PdscRecord).fields, options)}\n`
      : `export type ${name} = ${generateSchema(schema, options)};\n`;
  return new Generated(name, namespace, value);
}
