import { Direction, FetchedEntities } from '../types';

const findEntitiesToCollapse = (entityUrn: string, direction: Direction, fetchedEntities: FetchedEntities) => {
    const { [entityUrn]: collapsingEntity, ...entitiesButNodeMap } = fetchedEntities;
    const fullyFetchedEntities = Object.values(entitiesButNodeMap).filter((entity) => entity.fullyFetched);
    const directionChildren = direction === Direction.Upstream ? 'upstreamChildren' : 'downstreamChildren';

    // children who belong exclusively to collapsing entity within all fetched (expanded) entities
    const exclusiveChildren =
        collapsingEntity?.[directionChildren]?.filter((directionChild) => {
            return !fullyFetchedEntities.some((fetchedEntity) =>
                fetchedEntity[directionChildren]?.some(
                    (fetchedDirectionChild) => fetchedDirectionChild.entity.urn === directionChild.entity.urn,
                ),
            );
        }) ?? [];

    const entitiesToCollapse = [...exclusiveChildren];

    // looking for full fetched children, we need to collapse them too
    exclusiveChildren.forEach((exclusiveChild) => {
        const fullFetchedExclusiveChild = fullyFetchedEntities.find(
            (fullFetchedEntity) => fullFetchedEntity.urn === exclusiveChild.entity.urn,
        );

        if (!fullFetchedExclusiveChild) {
            return;
        }

        entitiesToCollapse.push(
            ...findEntitiesToCollapse(fullFetchedExclusiveChild.urn, direction, entitiesButNodeMap),
        );
    });

    return entitiesToCollapse;
};

const collapseLineageNode = (nodeUrn: string, direction: Direction, asyncEntities: FetchedEntities) => {
    const entitiesToCollapse = findEntitiesToCollapse(nodeUrn, direction, asyncEntities);
    const newEntities = { ...asyncEntities };

    entitiesToCollapse.forEach((child) => {
        delete newEntities[child.entity.urn];
    });

    newEntities[nodeUrn] = {
        ...newEntities[nodeUrn],
        fullyFetched: false,
    };

    return newEntities;
};

export default collapseLineageNode;
