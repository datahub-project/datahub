import {
    Container,
    DataPlatform,
    EntityType,
    FineGrainedLineage,
    Health,
    InputFields,
    Maybe,
    SchemaFieldDataType,
    SchemaMetadata,
    Status,
} from '../../types.generated';

export enum LineageAssetType {
    Column,
    Entity,
    DataProduct,
}

interface LineageAssetBase {
    name: string;
}

export interface ColumnAsset extends LineageAssetBase {
    type: LineageAssetType.Column;
    dataType?: SchemaFieldDataType;
    nativeDataType?: string | null;
}

export interface EntityAsset extends LineageAssetBase {
    type: LineageAssetType.Entity;
    entityType: EntityType;
}

export interface DataProductAsset extends LineageAssetBase {
    type: LineageAssetType.DataProduct;
    size?: number;
}

export type LineageAsset = ColumnAsset | EntityAsset | DataProductAsset;

export interface FetchedEntityV2 {
    urn: string;
    name: string;
    // name to be shown on expansion if available
    expandedName?: string;
    type: EntityType;
    subtype?: string;
    icon?: string;
    numUpstreamChildren?: number;
    numDownstreamChildren?: number;
    fullyFetched?: boolean;
    platform?: DataPlatform;
    status?: Maybe<Status>;
    fineGrainedLineages?: FineGrainedLineage[];
    schemaMetadata?: SchemaMetadata;
    inputFields?: InputFields;
    canEditLineage?: boolean;
    health?: Maybe<Health[]>;
    lineageAssets?: LineageAsset[];
    parentContainers?: Container[];
}
