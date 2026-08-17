import { GraphStoreFields, NodeContext } from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';
import { LineageVisualizationNode } from '@app/lineageV3/useComputeGraph/NodeBuilder';

import { EntityType } from '@types';

type Urn = string;

export type GraphStore = Pick<NodeContext, GraphStoreFields> & { rootType: EntityType };

export interface DataProductGroup {
    urn: Urn;
    entity?: FetchedEntityV2;
    colorHex?: string;
    memberUrns: Set<Urn>;
    /** Query nodes rendered inside this product's bounding box. Queries can't belong to a data
     * product; these are placed by `assignQueriesToGroups`, from the lineage they connect. */
    queryUrns: Set<Urn>;
}

export interface BoxLayout {
    group: DataProductGroup;
    /** Member and query nodes with data-product-qualified ids, positioned relative to the box. */
    memberNodes: LineageVisualizationNode[];
    /** Number of the product's own members shown, excluding query nodes placed inside the box. */
    memberCount: number;
    width: number;
    height: number;
}
