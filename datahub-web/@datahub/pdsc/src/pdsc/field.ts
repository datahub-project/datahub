export type SimpleField = 'boolean' | 'bytes' | 'double' | 'fixed' | 'float' | 'int' | 'long' | 'string';

/**
 * Generate the typescript for the type, or undefined if not a simple type.
 *
 * @param type
 */
export function generate(type: SimpleField): string | undefined {
  switch (type) {
    case 'boolean':
      return 'boolean';
    case 'bytes':
      return 'number';
    case 'double':
      return 'number';
    case 'float':
      return 'number';
    case 'fixed':
      return 'string';
    case 'int':
      return 'number';
    case 'long':
      return 'number';
    case 'string':
      return 'string';
    default:
      return;
  }
}
