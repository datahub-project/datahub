import { useContext, useEffect, useState } from 'react';

import { FetchStatus, LineageNodesContext } from '@app/lineageV3/common';
import { FetchedEntityV2 } from '@app/lineageV3/types';

import { GetBulkEntityDataProductsQuery, useGetBulkEntityDataProductsQuery } from '@graphql/lineage.generated';
import { EntityType, LineageDirection } from '@types';

// Matches MAX_URNS in BulkEntityDataProductsResolver
const BATCH_SIZE = 100;

type DataProductResult =
    GetBulkEntityDataProductsQuery['bulkEntityDataProducts']['entities'][number]['dataProductAssociations'][number]['dataProduct'];

/**
 * Fetches the data products containing each node in the graph, in batches. Stores minimal membership
 * (`node.containers`) on each node and the data products' display entities in
 * `containerEntities`, for the bounding boxes. Only active for the data product lineage graph,
 * where membership determines how a node is rendered; nodes are not displayed until it is known.
 */
export default function useBulkDataProductMemberships() {
    const { rootUrn, rootType, nodes, containerEntities, nodeVersion, dataVersion, setDataVersion } =
        useContext(LineageNodesContext);
    const skip = rootType !== EntityType.DataProduct;

    const [urnsToFetch, setUrnsToFetch] = useState<string[]>([]);
    useEffect(() => {
        if (skip) return;
        setUrnsToFetch((oldUrnsToFetch) => {
            const newUrnsToFetch = Array.from(nodes.values())
                // Query nodes cannot be in data products
                .filter((node) => node.containers === undefined && node.type !== EntityType.Query)
                .map((node) => node.urn)
                .slice(0, BATCH_SIZE);
            if (JSON.stringify(oldUrnsToFetch) !== JSON.stringify(newUrnsToFetch)) {
                return newUrnsToFetch;
            }
            return oldUrnsToFetch;
        });
    }, [skip, nodes, nodeVersion, dataVersion]);

    useGetBulkEntityDataProductsQuery({
        skip: skip || !urnsToFetch.length,
        fetchPolicy: 'cache-first',
        variables: { urns: urnsToFetch },
        onCompleted: (data) => {
            let changed = false;
            data?.bulkEntityDataProducts?.entities?.forEach((result) => {
                const node = nodes.get(result.urn);
                if (node && node.containers === undefined) {
                    node.containers = result.dataProductAssociations.map((association) => ({
                        urn: association.dataProduct.urn,
                        isOutputPort: association.isOutputPort,
                    }));
                    // Store each data product's display entity once, for its bounding box.
                    result.dataProductAssociations.forEach(({ dataProduct }) => {
                        if (!containerEntities.has(dataProduct.urn)) {
                            const entity = makeDataProductEntity(dataProduct);
                            if (entity) containerEntities.set(dataProduct.urn, entity);
                        }
                    });
                    // Members of the home data product — directly or via a sibling — are shown
                    // expanded, so their lineage renders without a manual expand.
                    if (node.containers.some((container) => container.urn === rootUrn)) {
                        node.isExpanded = {
                            [LineageDirection.Upstream]: true,
                            [LineageDirection.Downstream]: true,
                        };
                        node.fetchStatus = {
                            [LineageDirection.Upstream]: FetchStatus.COMPLETE,
                            [LineageDirection.Downstream]: FetchStatus.COMPLETE,
                        };
                    }
                    changed = true;
                }
            });
            if (changed) {
                setDataVersion((version) => version + 1);
            }
        },
    });
}

/** Builds a minimal FetchedEntityV2 for a data product, for rendering its bounding box. */
function makeDataProductEntity(dataProduct: DataProductResult): FetchedEntityV2 | undefined {
    const name = dataProduct.properties?.name;
    if (!name) return undefined;
    const domainEntity = dataProduct.domain?.domain;
    return {
        urn: dataProduct.urn,
        type: EntityType.DataProduct,
        name,
        exists: true,
        genericEntityProperties: {
            type: EntityType.DataProduct,
            domain: dataProduct.domain ?? undefined,
            parentDomains: domainEntity ? { domains: [domainEntity], count: 1 } : undefined,
        } as any,
    };
}
