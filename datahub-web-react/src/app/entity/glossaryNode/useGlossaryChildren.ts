import { useEffect, useState } from 'react';
import { Entity, EntityType, SortOrder } from '@src/types.generated';
import { useScrollAcrossEntitiesQuery } from '@src/graphql/search.generated';
import { ENTITY_INDEX_FILTER_NAME } from '@src/app/search/utils/constants';
import { ENTITY_NAME_FIELD } from '@src/app/searchV2/context/constants';
import { useInView } from 'react-intersection-observer';
import { useGlossaryEntityData } from '../shared/GlossaryEntityContext';

function getGlossaryChildrenScrollInput(urn: string, scrollId: string | null) {
    return {
        input: {
            scrollId,
            query: '*',
            types: [EntityType.GlossaryNode, EntityType.GlossaryTerm],
            orFilters: [{ and: [{ field: 'parentNode', values: [urn || ''] }] }],
            count: 50,
            sortInput: {
                sortCriteria: [
                    { field: ENTITY_INDEX_FILTER_NAME, sortOrder: SortOrder.Ascending },
                    { field: ENTITY_NAME_FIELD, sortOrder: SortOrder.Ascending },
                ],
            },
        },
    };
}

interface Props {
    entityUrn?: string;
    skip?: boolean;
}

export default function useGlossaryChildren({ entityUrn, skip }: Props) {
    const { nodeToNewEntity, setNodeToNewEntity, setNodeToDeletedUrn, nodeToDeletedUrn } = useGlossaryEntityData();
    const [scrollId, setScrollId] = useState<string | null>(null);
    const [dataUrnsSet] = useState<Set<string>>(new Set());
    const [data, setData] = useState<Entity[]>([]);
    const { data: scrollData, loading } = useScrollAcrossEntitiesQuery({
        variables: {
            ...getGlossaryChildrenScrollInput(entityUrn || '', scrollId),
        },
        skip: !entityUrn || skip,
        notifyOnNetworkStatusChange: true,
        onCompleted: (d) => {
            const newResults = d.scrollAcrossEntities?.searchResults
                .filter((r) => !dataUrnsSet.has(r.entity.urn))
                .map((r) => r.entity);
            if (newResults && newResults.length) {
                setData((currData) => [...currData, ...newResults]);
                newResults.forEach((r) => dataUrnsSet.add(r.urn));
            }
        },
    });
    const nextScrollId = scrollData?.scrollAcrossEntities?.nextScrollId;

    // update when new entity is added
    useEffect(() => {
        if (entityUrn && nodeToNewEntity[entityUrn] && !dataUrnsSet.has(nodeToNewEntity[entityUrn].urn)) {
            const newEntity = nodeToNewEntity[entityUrn];
            setData((currData) => [newEntity, ...currData]);
            dataUrnsSet.add(newEntity.urn);
            setNodeToNewEntity((currData) => {
                const currDataCopy = { ...currData };
                delete currDataCopy[entityUrn];
                return currDataCopy;
            });
        }
    }, [entityUrn, nodeToNewEntity, setNodeToNewEntity, dataUrnsSet]);

    // update when entity is removed
    useEffect(() => {
        if (entityUrn && nodeToDeletedUrn[entityUrn]) {
            const deletedUrn = nodeToDeletedUrn[entityUrn];
            setData((currData) => currData.filter((e) => e.urn !== deletedUrn));
            setNodeToDeletedUrn((currData) => {
                const currDataCopy = { ...currData };
                delete currDataCopy[entityUrn];
                return currDataCopy;
            });
        }
    }, [entityUrn, nodeToDeletedUrn, setNodeToDeletedUrn]);

    const [scrollRef, inView] = useInView({ triggerOnce: false });

    useEffect(() => {
        if (!loading && nextScrollId && scrollId !== nextScrollId && inView) {
            setScrollId(nextScrollId);
        }
    }, [inView, nextScrollId, scrollId, loading]);

    return { scrollRef, data, loading };
}
