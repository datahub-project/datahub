import { GlobalTags, SchemaField } from '@types';

export interface ExtendedSchemaFields extends SchemaField {
    children?: Array<ExtendedSchemaFields>;
    depth?: number;
    previousDescription?: string | null;
    pastGlobalTags?: GlobalTags | null;
    isNewRow?: boolean;
    isDeletedRow?: boolean;
    parent?: ExtendedSchemaFields;
}

export enum SchemaViewType {
    NORMAL,
    BLAME,
}
