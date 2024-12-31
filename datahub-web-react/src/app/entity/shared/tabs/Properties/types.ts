import { DataTypeEntity, Entity, StructuredPropertyEntity } from '../../../../../types.generated';

export interface ValueColumnData {
    value: string | number | null;
    entity: Entity | null;
}

export interface TypeData {
    type: string;
    nativeDataType: string;
}

export interface PropertyRow {
    displayName: string;
    qualifiedName: string;
    values?: ValueColumnData[];
    children?: PropertyRow[];
    childrenCount?: number;
    parent?: PropertyRow;
    depth?: number;
    type?: TypeData;
    dataType?: DataTypeEntity;
    isParentRow?: boolean;
    structuredProperty?: StructuredPropertyEntity;
    associatedUrn?: string;
}
