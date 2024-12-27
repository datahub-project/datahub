import { Operation } from '@src/types.generated';
import { uniq } from 'lodash';

export const getUniqueActorsFromOperations = (operations: Omit<Operation, 'lastUpdatedTimestamp'>[]): string[] => {
    return uniq(operations.filter((operation) => operation.actor).map((operation) => operation.actor || ''));
};
