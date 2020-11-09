import * as path from 'path';

import { packageToNamespace, typeToNamespace } from '../ts-emitter';
import { generate, SimpleField } from './field';

export type FieldType = SimpleField | string | Schema | Array<Schema | string>;

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

interface IContext {
  namespace?: string;
  fromTypeRef?: boolean;
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
    return `${name}: ${generateSchema(f.type, options).result};`;
  });
  // Add the optional field type, if there are already fields.
  if (options.addType && fields.length) {
    body.push(`${options.addType}?: string`);
  }
  return `{\n  ${body.join('\n  ')}\n}`;
}

enum GenerateSchemaNodeType {
  COMPLEX_SUBNODE
}

/**
 * Generate the typescript representation for the given type.GG
 *
 * @param type
 */
function generateSchema(
  type: FieldType,
  options: SchemaOptions,
  context?: IContext
): {
  result: string;
  metadata?: {
    fieldName: string;
    value: string;
    type: GenerateSchemaNodeType;
  };
} {
  const generator = (t: FieldType, context?: IContext) => generateSchema(t, options, context);
  if (Array.isArray(type)) {
    if (type.length === 0) {
      return { result: 'never' };
    }
    /**
     * Instead of:
     * type a = {a: boolean} | {b: string}
     *
     * better:
     * type a = {a?:boolean, b?:string};
     *
     * but edge case:
     *
     * type a = boolean | {a?:boolean, b?:string};
     */
    const splittedContent = type.map(t => generator(t, context));
    const complexSubschemas = splittedContent.filter(
      content => content.metadata?.type === GenerateSchemaNodeType.COMPLEX_SUBNODE
    );
    const everythingElse = splittedContent.filter(
      content => content.metadata?.type !== GenerateSchemaNodeType.COMPLEX_SUBNODE
    );
    const complexSubschemasString =
      '\n | {\n' +
      complexSubschemas.reduce((acc, schema) => {
        const { fieldName, value } = schema.metadata || {};
        return acc + `${fieldName}${complexSubschemas.length > 1 ? '?' : ''}: ${value},\n`;
      }, '') +
      '}\n';
    const joinedSubschemas = complexSubschemas.length > 0 ? complexSubschemasString : '';

    const content = everythingElse.map(content => content.result).join('\n | ') + joinedSubschemas;
    return { result: content };
  }
  if (isComplexType(type)) {
    const subType = type.type;
    switch (subType) {
      case 'array': {
        const items = (type as PdscArray).items;
        const translated = generator(items, context);
        if (typeof items === 'string') {
          return { result: `${translated.result}[]` };
        } else {
          return { result: `Array<${translated.result}>` };
        }
      }
      case 'enum': {
        let values = (type as PdscEnum).symbols;
        if (!values || !values.length) {
          values = ['NONE'];
        }
        return { result: values.map(a => `'${a}'`).join('\n | ') };
      }
      case 'map': {
        const values = (type as PdscMap).values;
        const translated = generator(values);
        return { result: `{[id: string]: ${translated.result}}` };
      }
      case 'record': {
        const fields = (type as PdscRecord).fields;

        return { result: `${generateRecordFields(fields, options)}` };
      }
      case 'typeref': {
        let ref = (type as PdscTyperef).ref;
        if (Array.isArray(ref)) {
          ref = ref.map(
            (str: string | Schema): Schema => {
              if (typeof str === 'string') {
                return { type: str } as Schema;
              }
              return str;
            }
          );
        }
        return generator(ref, { namespace: type.namespace, fromTypeRef: true });
      }
      default: {
        if (isComplexType(subType)) {
          return generator(subType);
        }
        if (typeof subType === 'string') {
          const simple = generate(subType as SimpleField);
          if (simple) {
            if ((type as PdscSubrecord).alias) {
              const fieldName = (type as PdscSubrecord).alias || '';
              const value = simple;
              return {
                result: `{ ${fieldName}: ${value} }`,
                metadata: { fieldName, value, type: GenerateSchemaNodeType.COMPLEX_SUBNODE }
              };
            } else {
              return { result: simple };
            }
          }

          // we have a package name
          const fieldName =
            (type as PdscSubrecord).alias ||
            `'${subType.indexOf('.') < 0 && context?.namespace ? context?.namespace + '.' : ''}${subType}'`;
          const value = typeToNamespace(subType, options.ignorePackages, context?.namespace);
          return {
            result: `{ ${fieldName}: ${value} }`,
            metadata: { fieldName, value, type: GenerateSchemaNodeType.COMPLEX_SUBNODE }
          };
        }
      }
    }
  }
  if (typeof type === 'string') {
    const simple = generate(type as SimpleField);

    if (simple) {
      return { result: simple };
    }

    // we have a package name
    return { result: typeToNamespace(type, options.ignorePackages, context?.namespace) };
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
      : `export type ${name} = ${generateSchema(schema, options).result};\n`;
  return new Generated(name, namespace, value);
}
