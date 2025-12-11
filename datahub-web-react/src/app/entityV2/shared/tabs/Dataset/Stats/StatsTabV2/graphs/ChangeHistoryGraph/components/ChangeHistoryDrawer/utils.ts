/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { uniq } from 'lodash';
import moment, { Moment } from 'moment';

import { Operation } from '@src/types.generated';

export const getUniqueActorsFromOperations = (operations: Omit<Operation, 'lastUpdatedTimestamp'>[]): string[] => {
    return uniq(operations.filter((operation) => operation.actor).map((operation) => operation.actor || ''));
};

export function dateStringToMoment(value: string | null | undefined): Moment | null {
    if (!value) return null;
    return moment(value);
}

export function momentToDateString(value: Moment | null | undefined): string | null {
    if (!value) return null;
    return value.format('YYYY-MM-DD');
}
