import { SchemaField, GlobalTags, ConstraintsList, Constraints } from '../../../../../../types.generated';

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
