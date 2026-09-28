import uniq from 'lodash/uniq';

import { Operation } from '@src/types.generated';
import dayjs from '@utils/dayjs';
import type { Dayjs } from '@utils/dayjs';

export const getUniqueActorsFromOperations = (operations: Omit<Operation, 'lastUpdatedTimestamp'>[]): string[] => {
    return uniq(operations.filter((operation) => operation.actor).map((operation) => operation.actor || ''));
};

export function parseDateString(value: string | null | undefined): Dayjs | null {
    if (!value) return null;
    return dayjs(value);
}

export function formatDateString(value: Dayjs | null | undefined): string | null {
    if (!value) return null;
    return value.format('YYYY-MM-DD');
}
