import { GenericEntityProperties } from '@app/entity/shared/types';

import {
    Chart,
    Dashboard,
    DataJob,
    DataPlatform,
    Dataset,
    Deprecation,
    EntityType,
    FineGrainedLineage,
    Health,
    InputFields,
    LineageRelationship,
    Maybe,
    MlFeature,
    MlFeatureTable,
    MlModel,
    MlModelGroup,
    MlPrimaryKey,
    SchemaFieldDataType,
    SchemaMetadata,
    ScrollResults,
    SiblingProperties,
    Status,
    StructuredProperties,
    VersionProperties,
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

interface EntityAsset extends LineageAssetBase {
    type: LineageAssetType.Entity;
    entityType: EntityType;
}

interface DataProductAsset extends LineageAssetBase {
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
    /** Schema fields this entity reads as a whole, having no columns of its own, e.g. a metric. */
    upstreamSchemaFieldUrns?: string[];
    schemaMetadata?: SchemaMetadata;
    inputFields?: InputFields;
    canEditLineage?: boolean;
    health?: Health[];
    versionProperties?: VersionProperties;
    lineageAssets?: Map<string, LineageAsset>;
    lineageSiblingIcon?: string;
    containers?: GenericEntityProperties[];
    parent?: GenericEntityProperties; // Schema field parent
    structuredProperties?: StructuredProperties;
    deprecation?: Deprecation;
    genericEntityProperties?: GenericEntityProperties;
}

export type FetchedEntity = {
    urn: string;
    name: string;
    // name to be shown on expansion if available
    expandedName?: string;
    type: EntityType;
    subtype?: string;
    icon?: string;
    siblingIcon?: string;
    // children?: Array<string>;
    upstreamChildren?: Array<EntityAndType>;
    upstreamRelationships?: Array<LineageRelationship>;
    numUpstreamChildren?: number;
    downstreamChildren?: Array<EntityAndType>;
    downstreamRelationships?: Array<LineageRelationship>;
    numDownstreamChildren?: number;
    fullyFetched?: boolean;
    platform?: DataPlatform;
    status?: Maybe<Status>;
    siblingPlatforms?: Maybe<DataPlatform[]>;
    fineGrainedLineages?: FineGrainedLineage[];
    upstreamSchemaFieldUrns?: string[];
    siblings?: Maybe<SiblingProperties>;
    siblingsSearch?: Maybe<ScrollResults>;
    schemaMetadata?: SchemaMetadata;
    inputFields?: InputFields;
    canEditLineage?: boolean;
    health?: Maybe<Health[]>;
    structuredProperties?: Maybe<StructuredProperties>;
    parents?: GenericEntityProperties[];
    parent?: GenericEntityProperties;
};

export type NodeData = {
    urn?: string;
    name: string;
    // name to be shown on expansion if available
    expandedName?: string;
    type?: EntityType;
    subtype?: string;
    children?: Array<NodeData>;
    unexploredChildren?: number;
    icon?: string;
    // Hidden children are unexplored but in the opposite direction of the flow of the graph.
    // Currently our visualization does not support expanding in two directions
    countercurrentChildrenUrns?: string[];
    platform?: DataPlatform;
    status?: Maybe<Status>;
    siblingPlatforms?: Maybe<DataPlatform[]>;
    schemaMetadata?: SchemaMetadata;
    inputFields?: InputFields;
    canEditLineage?: boolean;
    upstreamRelationships?: Array<LineageRelationship>;
    downstreamRelationships?: Array<LineageRelationship>;
    health?: Maybe<Health[]>;
    structuredProperties?: Maybe<StructuredProperties>;
    siblingStructuredProperties?: Maybe<StructuredProperties>;
};

export enum Direction {
    Upstream = 'Upstream',
    Downstream = 'Downstream',
}

export type EntityAndType =
    | {
          type: EntityType.Dataset;
          entity: Dataset;
      }
    | {
          type: EntityType.Chart;
          entity: Chart;
      }
    | {
          type: EntityType.Dashboard;
          entity: Dashboard;
      }
    | {
          type: EntityType.DataJob;
          entity: DataJob;
      }
    | {
          type: EntityType.MlfeatureTable;
          entity: MlFeatureTable;
      }
    | {
          type: EntityType.Mlfeature;
          entity: MlFeature;
      }
    | {
          type: EntityType.Mlmodel;
          entity: MlModel;
      }
    | {
          type: EntityType.MlmodelGroup;
          entity: MlModelGroup;
      }
    | {
          type: EntityType.MlprimaryKey;
          entity: MlPrimaryKey;
      };
