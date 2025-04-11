import { GenericEntityProperties } from '@app/entity/shared/types';
import { FullLineageResultsFragment } from '../../graphql/lineage.generated';
import {
    Chart,
    Dashboard,
    DataJob,
    DataPlatform,
    Dataset,
    Entity,
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
    SchemaMetadata,
    ScrollResults,
    SiblingProperties,
    Status,
    StructuredProperties,
} from '../../types.generated';

export type EntitySelectParams = {
    type: EntityType;
    urn: string;
};

export type LineageExpandParams = {
    type: EntityType;
    urn: string;
    direction: Direction;
};

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

export type VizNode = {
    x: number;
    y: number;
    data: NodeData;
    direction: Direction;
};

export type VizEdge = {
    source: VizNode;
    target: VizNode;
    curve: { x: number; y: number }[];
    sourceField?: string;
    targetField?: string;
    createdOn?: Maybe<number>;
    createdActor?: Maybe<Entity>;
    updatedOn?: Maybe<number>;
    updatedActor?: Maybe<Entity>;
    isManual?: boolean;
};

export type ColumnEdge = {
    sourceUrn: string;
    sourceField: string;
    targetUrn: string;
    targetField: string;
};

export type FetchedEntities = Map<string, FetchedEntity>;

export enum Direction {
    Upstream = 'Upstream',
    Downstream = 'Downstream',
}

export type LineageExplorerParams = {
    type: string;
    urn: string;
};

export type TreeProps = {
    margin?: { top: number; right: number; bottom: number; left: number };
    entityAndType?: EntityAndType | null;
    fetchedEntities: Map<string, FetchedEntity>;
    onEntityClick: (EntitySelectParams) => void;
    onEntityCenter: (EntitySelectParams) => void;
    onLineageExpand: (data: EntityAndType) => void;
    selectedEntity?: EntitySelectParams;
    hoveredEntity?: EntitySelectParams;
    fineGrainedMap?: any;
};

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

export interface LineageResult {
    urn: string;
    upstream?: Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
    downstream?: Maybe<{ __typename?: 'EntityLineageResult' } & FullLineageResultsFragment>;
}

export interface UpdatedLineages {
    [urn: string]: UpdatedLineage;
}

export interface UpdatedLineage {
    lineageDirection: Direction;
    entitiesToAdd: Entity[];
    urnsToRemove: string[];
}
