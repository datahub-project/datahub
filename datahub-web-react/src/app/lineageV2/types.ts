import { GenericEntityProperties } from '@app/entity/shared/types';
import {
    DataPlatform,
    Deprecation,
    EntityType,
    FineGrainedLineage,
    Health,
    InputFields,
    LineageRelationship,
    SchemaFieldDataType,
    SchemaMetadata,
    Status,
    StructuredProperties,
} from '@types';

export enum LineageAssetType {
    Column,
    Entity,
    DataProduct,
}

interface LineageAssetBase {
    name: string;
    numUpstream?: number;
    numDownstream?: number;
    lineageCountsFetched?: boolean;
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

export type FetchedEntityV2Relationship = LineageRelationship & { urn: string }; // Destination urn

export interface FetchedEntityV2 {
    urn: string;
    type: EntityType;
    exists?: boolean;
    name: string;
    // name to be shown on expansion if available
    expandedName?: string;
    subtype?: string;
    icon?: string;
    numUpstreamChildren?: number;
    numDownstreamChildren?: number;
    upstreamRelationships?: FetchedEntityV2Relationship[];
    downstreamRelationships?: FetchedEntityV2Relationship[];
    fullyFetched?: boolean;
    platform?: DataPlatform;
    status?: Status;
    fineGrainedLineages?: FineGrainedLineage[];
    schemaMetadata?: SchemaMetadata;
    inputFields?: InputFields;
    canEditLineage?: boolean;
    health?: Health[];
    lineageAssets?: Map<string, LineageAsset>;
    lineageSiblingIcon?: string;
    containers?: GenericEntityProperties[];
    parent?: GenericEntityProperties; // Schema field parent
    structuredProperties?: StructuredProperties;
    deprecation?: Deprecation;
}
