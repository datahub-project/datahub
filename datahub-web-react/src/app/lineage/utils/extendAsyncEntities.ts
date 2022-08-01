import { SchemaFieldRef } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';
import { EntityAndType, FetchedEntities } from '../types';

const breakFieldUrn = (ref: SchemaFieldRef) => {
    const before = ref.urn;
    const after = ref.path;

    return [before, after];
};

const setEdge = (fineGrainedMap, upstreamEntityUrn, upstreamField, downstreamEntityUrn, downstreamField) => {
    console.log(fineGrainedMap);
    const mapForUrn = fineGrainedMap.forward[upstreamEntityUrn] || {};
    const mapForField = mapForUrn[upstreamField] || {};
    const listForDownstream = mapForField[downstreamEntityUrn] || [];
    listForDownstream.push(downstreamField);

    // eslint-disable-next-line no-param-reassign
    fineGrainedMap.forward[upstreamEntityUrn] = mapForUrn;
    mapForUrn[upstreamField] = mapForField;
    mapForField[downstreamEntityUrn] = listForDownstream;

    const mapForUrnReverse = fineGrainedMap.reverse[downstreamEntityUrn] || {};
    const mapForFieldReverse = mapForUrnReverse[downstreamField] || {};
    const listForDownstreamReverse = mapForFieldReverse[upstreamEntityUrn] || [];
    listForDownstreamReverse.push(upstreamField);

    // eslint-disable-next-line no-param-reassign
    fineGrainedMap.reverse[downstreamEntityUrn] = mapForUrnReverse;
    mapForUrnReverse[downstreamField] = mapForFieldReverse;
    mapForFieldReverse[upstreamEntityUrn] = listForDownstreamReverse;
};

export default function extendAsyncEntities(
    fineGrainedMap: any,
    fetchedEntities: FetchedEntities,
    entityRegistry: EntityRegistry,
    entityAndType: EntityAndType,
    fullyFetched = false,
): FetchedEntities {
    if (fetchedEntities[entityAndType.entity.urn]?.fullyFetched) {
        return fetchedEntities;
    }

    const lineageVizConfig = entityRegistry.getLineageVizConfig(entityAndType.type, entityAndType.entity);

    if (!lineageVizConfig) return fetchedEntities;

    if (lineageVizConfig.fineGrainedLineages && lineageVizConfig.fineGrainedLineages.length > 0) {
        lineageVizConfig.fineGrainedLineages.forEach((fineGrainedLineage) => {
            fineGrainedLineage.upstreams?.forEach((upstream) => {
                const [upstreamEntityUrn, upstreamField] = breakFieldUrn(upstream);
                fineGrainedLineage.downstreams?.forEach((downstream) => {
                    const [downstreamEntityUrn, downstreamField] = breakFieldUrn(downstream);
                    setEdge(fineGrainedMap, upstreamEntityUrn, upstreamField, downstreamEntityUrn, downstreamField);
                });
            });
        });
    }

    return {
        ...fetchedEntities,
        [entityAndType.entity.urn]: {
            ...lineageVizConfig,
            fullyFetched,
        },
    };
}
