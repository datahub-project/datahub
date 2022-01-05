import { SchemaField, GlobalTags } from '../../../../../../types.generated';

/** Data Quality- Result Metric based on the constraints applied */
export type ResultMetrics = {
    __typename?: 'ResultMetrics';
    /** time series date value for when the data quality check completed */
    timestamp: string;
    /** count of records which satisfy the constraint applied */
    elementCount: number;
    /** count of records which satisfy unexpected count of the constraint applied */
    unexpectedCount: number;
    /** count of records which satisfy unexpected precent of the constraint applied */
    unexpectedPercent: number;
    /** count of records which satisfy missing count of the constraint applied */
    missingCount: number;
};

/** Data Quality- Constraints  */
export type Constraints = {
    __typename?: 'Constraints';
    /** constraints type which is applied */
    constraint: string;
    /** result metrics for which the constraint is applied */
    resultMetrics: Array<ResultMetrics>;
};

/** Data Quality- Constraints list array */
export type ConstraintsList = {
    __typename?: 'ConstraintsList';
    /** constraint list to get the result metric */
    constraints: Array<Constraints>;
};

export interface ExtendedSchemaFields extends SchemaField {
    children?: Array<ExtendedSchemaFields>;
    depth?: number;
    previousDescription?: string | null;
    pastGlobalTags?: GlobalTags | null;
    isNewRow?: boolean;
    isDeletedRow?: boolean;
}

export interface ExtendedDataQualitySchemaFields extends ExtendedSchemaFields {
    constraints?: ConstraintsList;
}
