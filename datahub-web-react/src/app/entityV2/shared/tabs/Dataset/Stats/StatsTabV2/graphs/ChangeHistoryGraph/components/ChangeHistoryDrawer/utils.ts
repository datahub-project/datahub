import { uniq } from 'lodash';
import dayjs from 'dayjs';
import type { Dayjs } from 'dayjs';

import { Operation } from '@src/types.generated';

export const getUniqueActorsFromOperations = (operations: Omit<Operation, 'lastUpdatedTimestamp'>[]): string[] => {
    return uniq(operations.filter((operation) => operation.actor).map((operation) => operation.actor || ''));
};

export function dateStringToMoment(value: string | null | undefined): Dayjs | null {
    if (!value) return null;
    return dayjs(value);
}

export function momentToDateString(value: Dayjs | null | undefined): string | null {
    if (!value) return null;
    return value.format('YYYY-MM-DD');
}
