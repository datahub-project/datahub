import { OperationType } from '@src/types.generated';

export enum AggregationGroup {
    Purple = 'PURPLE',
    Red = 'RED',
}

export type Operation = {
    value: number;
    type: OperationType;
    customType?: CustomOperationType;
    name: string;
    key: string;
    // Identify aggregation group (for color accessors)
    group: AggregationGroup;
};

export type CustomOperationType = string;
export type AnyOperationType = OperationType | CustomOperationType;

export type CustomOperations = { [key: CustomOperationType]: Operation };

export type OperationsData = {
    summary: {
        totalOperations: number;
        totalCustomOperations: number;
        mom: number | null;
    };
    operations: {
        inserts: Operation;
        updates: Operation;
        deletes: Operation;
        alters: Operation;
        creates: Operation;
        drops: Operation;
    } & CustomOperations;
};
