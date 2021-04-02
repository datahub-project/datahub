import { GetDatasetQuery } from '../../graphql/dataset.generated';
import { EntityType } from '../../types.generated';

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
    type: EntityType;
    // children?: Array<string>;
    upstreamChildren?: Array<string>;
    downstreamChildren?: Array<string>;
    fullyFetched: boolean;
};

export type NodeData = {
    urn?: string;
    name: string;
    type?: EntityType;
    children?: Array<NodeData>;
    unexploredChildren?: number;
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
    dataset: GetDatasetQuery['dataset'];
    fetchedEntities: { [x: string]: FetchedEntity };
    onEntityClick: (EntitySelectParams) => void;
    onLineageExpand: (LineageExpandParams) => void;
    selectedEntity?: EntitySelectParams;
    hoveredEntity?: EntitySelectParams;
};
