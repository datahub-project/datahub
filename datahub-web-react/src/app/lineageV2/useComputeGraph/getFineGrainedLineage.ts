import EntityRegistry from '@app/entityV2/EntityRegistry';
import {
    ColumnRef,
    createColumnRef,
    createEdgeId,
    createFineGrainedOperationRef,
    FineGrainedLineage,
    FineGrainedLineageMap,
    FineGrainedOperationRef,
    isUrnTransformational,
    NodeContext,
    parseColumnRef,
    setDefault,
} from '@app/lineageV2/common';
import { getFieldPathFromSchemaFieldUrn, getSourceUrnFromSchemaFieldUrn } from '@src/app/entityV2/schemaField/utils';
import { FineGrainedOperation } from '@app/sharedV2/EntitySidebarContext';
import { EntityType } from '@types';

export interface FineGrainedLineageData {
    indirect: FineGrainedLineage;
    fineGrainedOperations: Map<FineGrainedOperationRef, FineGrainedOperation>;
}

interface TentativeEdge {
    upstreamRef: ColumnRef;
    downstreamRef: ColumnRef;
    queryRef?: ColumnRef;
    operationRef?: FineGrainedOperationRef;
}

/**
 * Piece together column-level lineage directly from aspects,
 * e.g. dataset upstreamLineage, chart inputFields, and datajob dataJobInputOutput
 *
 *
 * @param context
 * @param entityRegistry
 * @return A map of column -> column edges, enhanced with information about the query for each edge.
 */
export default function getFineGrainedLineage(
    context: Pick<NodeContext, 'nodes' | 'edges'>,
    entityRegistry: EntityRegistry,
): FineGrainedLineageData {
    const { nodes, edges } = context;

    const indirect: FineGrainedLineage = { downstream: new Map(), upstream: new Map() };
    const fineGrainedOperations: Map<FineGrainedOperationRef, FineGrainedOperation> = new Map();
    // CLL that may be deduplicated, if there exists a column-level path through transformational nodes
    const tentativeEdges: TentativeEdge[] = [];

    function processEdge(
        upstreamUrn: string,
        upstreamField: string,
        downstreamUrn: string,
        downstreamField: string,
        intermediateIsQuery?: boolean,
        intermediateRef?: ColumnRef,
        operationRef?: FineGrainedOperationRef,
    ) {
        const upstreamRef = createColumnRef(upstreamUrn, upstreamField);
        const downstreamRef = createColumnRef(downstreamUrn, downstreamField);
        // Drop ghost edges and self edges
        if (!nodes.has(upstreamUrn) || !nodes.has(downstreamUrn) || upstreamRef === downstreamRef) return;

        if (
            edges.get(createEdgeId(upstreamUrn, downstreamUrn))?.isDisplayed ||
            (!intermediateIsQuery && intermediateRef)
        ) {
            addFineGrainedEdges(indirect, upstreamRef, downstreamRef, intermediateRef, operationRef);
        } else {
            tentativeEdges.push({ upstreamRef, downstreamRef, queryRef: intermediateRef, operationRef });
        }
    }

    nodes.forEach((node) => {
        node.entity?.fineGrainedLineages?.forEach((entry) => {
            let operationRef: FineGrainedOperationRef | undefined;
            let queryRef: ColumnRef | undefined;
            const intermediateNode = entry.query || (node.entity?.type === EntityType.DataJob && node.entity.urn);
            if (intermediateNode) {
                operationRef = createFineGrainedOperationRef(intermediateNode, entry.upstreams, entry.downstreams);
                queryRef = createColumnRef(intermediateNode, operationRef);
                fineGrainedOperations.set(operationRef, {
                    inputColumns: entry.upstreams?.map((ref) => [ref.urn, ref.path]) || undefined,
                    outputColumns: entry.downstreams?.map((ref) => [ref.urn, ref.path]) || undefined,
                    transformOperation: entry.transformOperation || undefined,
                });
            }
            entry.upstreams?.forEach((upstream) => {
                entry.downstreams?.forEach((downstream) => {
                    processEdge(
                        upstream.urn,
                        upstream.path,
                        downstream.urn,
                        downstream.path,
                        !!entry.query,
                        queryRef,
                        operationRef,
                    );
                });
            });
        });
        node.entity?.inputFields?.fields?.forEach((input) => {
            // Upstream of chart's field `schemaField` comes in as `schemaFieldUrn`
            if (input?.schemaFieldUrn && input?.schemaField) {
                const upstreamUrn = getSourceUrnFromSchemaFieldUrn(input.schemaFieldUrn);
                const upstreamField = getFieldPathFromSchemaFieldUrn(input.schemaFieldUrn);
                processEdge(upstreamUrn, upstreamField, node.urn, input.schemaField.fieldPath);
            }
        });
    });

    tentativeEdges
        .filter(
            ({ upstreamRef, downstreamRef }) =>
                !isTransformationalPath(indirect.downstream, upstreamRef, downstreamRef, entityRegistry),
        )
        .forEach(({ upstreamRef, downstreamRef, queryRef, operationRef }) => {
            addFineGrainedEdges(indirect, upstreamRef, downstreamRef, queryRef, operationRef);
        });

    return {
        indirect,
        fineGrainedOperations,
    };
}

function addFineGrainedEdges(
    fgl: FineGrainedLineage,
    upstreamRef: ColumnRef,
    downstreamRef: ColumnRef,
    queryRef?: ColumnRef,
    operationRef?: FineGrainedOperationRef,
) {
    if (queryRef) {
        setDefault(fgl.upstream, downstreamRef, new Map()).set(queryRef, operationRef);
        setDefault(fgl.upstream, queryRef, new Map()).set(upstreamRef, null);
        setDefault(fgl.downstream, upstreamRef, new Map()).set(queryRef, operationRef);
        setDefault(fgl.downstream, queryRef, new Map()).set(downstreamRef, null);
    } else {
        setDefault(fgl.upstream, downstreamRef, new Map()).set(upstreamRef, null);
        setDefault(fgl.downstream, upstreamRef, new Map()).set(downstreamRef, null);
    }
}

function isTransformationalPath(
    downstreamMap: FineGrainedLineageMap,
    upstreamRef: ColumnRef,
    downstreamRef: ColumnRef,
    entityRegistry: EntityRegistry,
): boolean {
    const stack = [upstreamRef];
    const seen = new Set<string>(stack);
    for (let node = stack.pop(); node; node = stack.pop()) {
        const found = Array.from(downstreamMap.get(node)?.keys() || []).some((childRef) => {
            if (childRef === downstreamRef) return true;
            if (!seen.has(childRef)) {
                const [childUrn] = parseColumnRef(childRef);
                if (isUrnTransformational(childUrn, entityRegistry)) {
                    stack.push(childRef);
                    seen.add(childRef);
                }
            }
            return false;
        });
        if (found) {
            return true;
        }
    }
    return false;
}
