import { FullLineageResultsFragment } from '../../graphql/lineage.generated';
import {
    Chart,
    Dashboard,
    DataJob,
    Dataset,
    EntityType,
    MlFeatureTable,
    MlPrimaryKey,
    MlFeature,
    MlModel,
    MlModelGroup,
    Maybe,
    Status,
    DataPlatform,
    FineGrainedLineage,
    SchemaMetadata,
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
    // children?: Array<string>;
    upstreamChildren?: Array<EntityAndType>;
    numUpstreamChildren?: number;
    downstreamChildren?: Array<EntityAndType>;
    numDownstreamChildren?: number;
    fullyFetched?: boolean;
    platform?: DataPlatform;
    status?: Maybe<Status>;
    siblingPlatforms?: Maybe<DataPlatform[]>;
    fineGrainedLineages?: [FineGrainedLineage];
    schemaMetadata?: SchemaMetadata;
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
    sourceField?: string;
    targetField?: string;
    curve: { x: number; y: number }[];
};

export type ColumnEdge = {
    sourceUrn: string;
    sourceField: string;
    targetUrn: string;
    targetField: string;
};

export type FetchedEntities = { [x: string]: FetchedEntity };

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
    fetchedEntities: { [x: string]: FetchedEntity };
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
