import { GraphStoreFields, NodeContext } from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';

import { EntityType } from '@types';

type Urn = string;

export type GraphStore = Pick<NodeContext, GraphStoreFields> & { rootType: EntityType };

export interface BoundingBoxGroup {
    urn: Urn;
    /** Entity type of the bounding box (DataProduct, SemanticModel, etc.). */
    type: EntityType;
    entity?: FetchedEntityV2;
    colorHex?: string;
    memberUrns: Set<Urn>;
    /** Query nodes rendered in this bounding box; see `assignQueriesToGroups`. */
    queryUrns: Set<Urn>;
}

export interface BoxLayout {
    group: BoundingBoxGroup;
    /** Member and query nodes with bounding-box-qualified ids, positioned relative to the box. */
    memberNodes: LineageVisualizationNode[];
    /** Number of members shown, excluding query nodes. */
    memberCount: number;
    width: number;
    height: number;
}
