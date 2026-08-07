import { GraphStoreFields, NodeContext } from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';

import { EntityType } from '@types';

type Urn = string;

export type GraphStore = Pick<NodeContext, GraphStoreFields> & { rootType: EntityType };

export interface LineageContainerGroup {
    urn: Urn;
    /** Entity type of the container (DataProduct, SemanticModel etc.). */
    type: EntityType;
    entity?: FetchedEntityV2;
    colorHex?: string;
    memberUrns: Set<Urn>;
}

export interface BoxLayout {
    group: LineageContainerGroup;
    /** Member nodes with container-qualified ids, positioned relative to the bounding box. */
    memberNodes: LineageVisualizationNode[];
    width: number;
    height: number;
}
