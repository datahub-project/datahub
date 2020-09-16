import isPrimitive from '@nacho-ui/core/utils/lib/is-primitive';
import { isArray } from '@ember/array';

/**
 * A slightly more useful than 'isPrimitive()' function as this can be used if a value will show up properly when rendered to the
 * template. This means that arrays of primitives can actually be included as well as instances of String and Number
 * @param value - any value we want to test
 */
export default function isTemplatable(value: unknown): boolean {
  return (
    isPrimitive(value) ||
    (isArray(value) && isPrimitive(value[0])) ||
    value instanceof String ||
    value instanceof Number
  );
}
