/**
 * Length / precision / scale derived from a field's native type string.
 *
 * DataHub's schema model has no first-class length, precision or scale; relational sources carry
 * them inside `nativeDataType` (`VARCHAR(255)`, `DECIMAL(18,2)`, `NUMBER(18)`, `TIMESTAMP(6)`).
 * The normalized `type` decides how a single parameter is read: a length for string/binary
 * families, a precision for numeric/temporal ones. Anything unparseable yields no parts (blank
 * cells) rather than a guess.
 */

export interface TypeParts {
    length?: number;
    precision?: number;
    scale?: number;
}

const PARAMS = /\(\s*(\d+)\s*(?:,\s*(-?\d+)\s*)?\)/;
const LENGTH_FAMILY_NAMES = /char|string|text|binary|blob|clob|bit|raw|graphic/i;

/** Normalized SchemaFieldDataType names that take a length rather than a precision. */
const LENGTH_TYPES = new Set(['STRING', 'BYTES']);
/** Normalized names whose single parameter is a precision. */
const PRECISION_TYPES = new Set(['NUMBER', 'TIME', 'DATE']);

export function deriveTypeParts(nativeDataType?: string | null, normalizedType?: string | null): TypeParts {
    if (!nativeDataType) return {};
    const match = PARAMS.exec(nativeDataType);
    if (!match) return {};
    const first = Number(match[1]);
    const second = match[2] !== undefined ? Number(match[2]) : undefined;
    if (!Number.isFinite(first)) return {};

    if (second !== undefined) return { precision: first, scale: second };

    const normalized = normalizedType?.toUpperCase();
    if (normalized && LENGTH_TYPES.has(normalized)) return { length: first };
    if (normalized && PRECISION_TYPES.has(normalized)) return { precision: first };
    // No usable normalized type: fall back to the native type's family name.
    const name = nativeDataType.slice(0, match.index);
    return LENGTH_FAMILY_NAMES.test(name) ? { length: first } : { precision: first };
}

/** `18,2` / `18` / '' — how the Precision / Scale column prints its parts. */
export function formatPrecisionScale(parts: TypeParts): string {
    if (parts.precision === undefined) return '';
    return parts.scale === undefined ? String(parts.precision) : `${parts.precision},${parts.scale}`;
}
