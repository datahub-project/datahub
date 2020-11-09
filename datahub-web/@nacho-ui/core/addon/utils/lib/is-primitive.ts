/**
 * Non-deprecated version of the isPrimitive function available from 'util' library
 */
export default function isPrimitive(value: unknown): boolean {
  return (typeof value !== 'object' && typeof value !== 'function') || value === null;
}
