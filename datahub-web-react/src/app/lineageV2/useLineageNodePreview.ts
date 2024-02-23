import { useContext, useEffect } from 'react';
import { useGetLineagePreviewQuery } from '../../graphql/lineage.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import { LineageNodesContext } from './common';
import { setDifference } from './useColumnHighlighting';

export default function useLineageNodePreview(shownUrns: string[]) {
    const { nodes, setDataVersion } = useContext(LineageNodesContext);
    const entityRegistry = useEntityRegistry();

    const urnsToFetch = setDifference(new Set(nodes.keys()), new Set(shownUrns));

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
