import { useContext, useEffect, useMemo } from 'react';
import { useGetLineagePreviewQuery } from '../../graphql/lineage.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import { LineageNodesContext, setDifference } from './common';

export default function useLineageNodePreview(shownUrns: string[]) {
    const { nodes, setDataVersion } = useContext(LineageNodesContext);
    const entityRegistry = useEntityRegistry();

    // TODO: Do not refetch nodes that are in the process of being fetched by a previous call
    const urnsToFetch = useMemo(
        () => setDifference(new Set(nodes.keys()), new Set(shownUrns)).filter((urn) => !nodes.get(urn)?.backupEntity),
        [nodes, shownUrns],
    );

    const { data } = useGetLineagePreviewQuery({
        skip: !urnsToFetch.length,
        variables: {
            urns: urnsToFetch,
        },
    });

    useEffect(() => {
        if (data?.entities?.length) {
            data.entities.forEach((rawEntity) => {
                if (!rawEntity) {
                    return;
                }
                const entity = entityRegistry.getGenericEntityProperties(rawEntity.type, rawEntity);
                const node = nodes.get(rawEntity.urn);
                if (node) {
                    node.backupEntity = entity || undefined;
                }
            });
            setDataVersion((prev) => prev + 1);
        }
    }, [data, entityRegistry, nodes, setDataVersion]);
}
