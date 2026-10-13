import { VariantProps } from '@components/components/DatePicker/types';

const TIME_FORMAT = 'HH:mm:ss';

// dayjs hour/minute/second tokens. Month and day are uppercase M and D, so date-only
// formats like 'YYYY-MM-DD' don't match.
const HAS_TIME_TOKEN = /[Hhms]/;

/** Adds a time portion to a date format for `showTime`. */
export function buildDateTimeFormat(format: VariantProps['format']): string[] {
    const formats = (Array.isArray(format) ? format : [format]).filter(
        (entry): entry is string => typeof entry === 'string' && entry.length > 0,
    );

    if (formats.length === 0) return [`YYYY-MM-DD ${TIME_FORMAT}`];
    if (formats.some((entry) => HAS_TIME_TOKEN.test(entry))) return formats;

    return [...formats.map((entry) => `${entry} ${TIME_FORMAT}`), ...formats];
}
