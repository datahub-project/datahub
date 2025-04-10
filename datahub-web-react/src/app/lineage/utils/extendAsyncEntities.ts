import { EntityType, SchemaFieldRef } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';
import { EntityAndType, FetchedEntities, FetchedEntity } from '../types';
import {
    decodeSchemaField,
    getFieldPathFromSchemaFieldUrn,
    getSourceUrnFromSchemaFieldUrn,
    isSameColumn,
} from './columnLineageUtils';

const breakFieldUrn = (ref: SchemaFieldRef) => {
    const before = ref.urn;
    const after = decodeSchemaField(ref.path);

    return [before, after];
};

function updateFineGrainedMap(
    fineGrainedMap: any,
    upstreamEntityUrn: string,
    upstreamField: string,
    downstreamEntityUrn: string,
    downstreamField: string,
) {
    // ignore self-referential CLL fields
    if (
        isSameColumn({
            sourceUrn: upstreamEntityUrn,
            targetUrn: downstreamEntityUrn,
            sourceField: upstreamField,
            targetField: downstreamField,
        })
    ) {
        return;
    }
    const mapForUrn = fineGrainedMap.forward[upstreamEntityUrn] || {};
    const mapForField = mapForUrn[upstreamField] || {};
    const listForDownstream = [...(mapForField[downstreamEntityUrn] || [])];
    listForDownstream.push(downstreamField);

    // eslint-disable-next-line no-param-reassign
    fineGrainedMap.forward[upstreamEntityUrn] = mapForUrn;
    mapForUrn[upstreamField] = mapForField;
    mapForField[downstreamEntityUrn] = listForDownstream;

    const mapForUrnReverse = fineGrainedMap.reverse[downstreamEntityUrn] || {};
    const mapForFieldReverse = mapForUrnReverse[downstreamField] || {};
    const listForDownstreamReverse = [...(mapForFieldReverse[upstreamEntityUrn] || [])];
    listForDownstreamReverse.push(upstreamField);

    // eslint-disable-next-line no-param-reassign
    fineGrainedMap.reverse[downstreamEntityUrn] = mapForUrnReverse;
    mapForUrnReverse[downstreamField] = mapForFieldReverse;
    mapForFieldReverse[upstreamEntityUrn] = listForDownstreamReverse;
}

export function extendColumnLineage(
    lineageVizConfig: FetchedEntity,
    fineGrainedMap: any,
    fineGrainedMapForSiblings: any,
    fetchedEntities: FetchedEntities,
) {
    if (lineageVizConfig.fineGrainedLineages && lineageVizConfig.fineGrainedLineages.length > 0) {
        lineageVizConfig.fineGrainedLineages.forEach((fineGrainedLineage) => {
            fineGrainedLineage.upstreams?.forEach((upstream) => {
                const [upstreamEntityUrn, upstreamField] = breakFieldUrn(upstream);

                if (lineageVizConfig.type === EntityType.DataJob) {
                    // draw a line from upstream dataset field to datajob
                    updateFineGrainedMap(
                        fineGrainedMap,
                        upstreamEntityUrn,
                        upstreamField,
                        lineageVizConfig.urn,
                        upstreamField,
                    );
                }

                fineGrainedLineage.downstreams?.forEach((downstream) => {
                    const [downstreamEntityUrn, downstreamField] = breakFieldUrn(downstream);

                    if (lineageVizConfig.type === EntityType.DataJob) {
                        // draw line from datajob upstream field to downstream fields
                        updateFineGrainedMap(
                            fineGrainedMap,
                            lineageVizConfig.urn,
                            upstreamField,
                            downstreamEntityUrn,
                            downstreamField,
                        );
                    } else {
                        // fineGrainedLineage always belongs on the downstream urn with upstreams pointing to another entity
                        // pass in the visualized node's urn and not the urn from the schema field as the downstream urn,
                        // as they will either be the same or if they are different, it belongs to a "hidden" sibling
                        updateFineGrainedMap(
                            fineGrainedMap,
                            upstreamEntityUrn,
                            upstreamField,
                            lineageVizConfig.urn,
                            downstreamField,
                        );
                    }

                    // upstreamEntityUrn could belong to a sibling we don't "render", so store its inputs to updateFineGrainedMap
                    // and update the fine grained map later when we see the entity with these siblings
                    // eslint-disable-next-line no-param-reassign
                    fineGrainedMapForSiblings[upstreamEntityUrn] = [
                        ...(fineGrainedMapForSiblings[upstreamEntityUrn] || []),
                        {
                            upstreamField,
                            downstreamEntityUrn: lineageVizConfig.urn,
                            downstreamField,
                        },
                    ];

                    // if this upstreamEntityUrn is a sibling of one of the already rendered nodes,
                    // update the fine grained map with the rendered node instead of its sibling
                    Array.from(fetchedEntities.keys()).forEach((urn) => {
                        fetchedEntities.get(urn)?.siblingsSearch?.searchResults?.forEach((sibling) => {
                            if (sibling && sibling.entity.urn === upstreamEntityUrn) {
                                updateFineGrainedMap(
                                    fineGrainedMap,
                                    urn,
                                    upstreamField,
                                    lineageVizConfig.urn,
                                    downstreamField,
                                );
                            }
                        });
                    });
                });
            });
            if (lineageVizConfig.type === EntityType.DataJob && !fineGrainedLineage.upstreams?.length) {
                fineGrainedLineage.downstreams?.forEach((downstream) => {
                    const [downstreamEntityUrn, downstreamField] = breakFieldUrn(downstream);
                    updateFineGrainedMap(
                        fineGrainedMap,
                        lineageVizConfig.urn,
                        downstreamField,
                        downstreamEntityUrn,
                        downstreamField,
                    );
                });
            }
        });
    }

    // if we've seen fineGrainedMappings for this current entity's siblings, update the
    // fine grained map with the rendered urn instead of the "hidden" sibling urn
    lineageVizConfig.siblingsSearch?.searchResults?.forEach((sibling) => {
        if (sibling && fineGrainedMapForSiblings[sibling.entity.urn]) {
            fineGrainedMapForSiblings[sibling.entity.urn].forEach((entry) => {
                updateFineGrainedMap(
                    fineGrainedMap,
                    lineageVizConfig.urn,
                    entry.upstreamField,
                    entry.downstreamEntityUrn,
                    entry.downstreamField,
                );
            });
        }
    });

    // below is to update the fineGrainedMap for Data Jobs
    if (lineageVizConfig.inputFields?.fields && lineageVizConfig.inputFields.fields.length > 0) {
        lineageVizConfig.inputFields.fields.forEach((inputField) => {
            if (inputField?.schemaFieldUrn && inputField.schemaField) {
                const sourceUrn = getSourceUrnFromSchemaFieldUrn(inputField.schemaFieldUrn);
                if (sourceUrn !== lineageVizConfig.urn) {
                    updateFineGrainedMap(
                        fineGrainedMap,
                        sourceUrn,
                        getFieldPathFromSchemaFieldUrn(inputField.schemaFieldUrn),
                        lineageVizConfig.urn,
                        inputField.schemaField.fieldPath,
                    );
                }
            }
        });
    }
}

export default function extendAsyncEntities(
    fineGrainedMap: any,
    fineGrainedMapForSiblings: any,
    fetchedEntities: FetchedEntities,
    entityRegistry: EntityRegistry,
    entityAndType: EntityAndType,
    fullyFetched = false,
): FetchedEntities {
    if (fetchedEntities.get(entityAndType.entity.urn)?.fullyFetched) {
        return fetchedEntities;
    }

    const lineageVizConfig = entityRegistry.getLineageVizConfig(entityAndType.type, entityAndType.entity);

    if (!lineageVizConfig) return fetchedEntities;

    extendColumnLineage(lineageVizConfig, fineGrainedMap, fineGrainedMapForSiblings, fetchedEntities);

    const newFetchedEntities = new Map(fetchedEntities);
    newFetchedEntities.set(entityAndType.entity.urn, { ...lineageVizConfig, fullyFetched });
    return newFetchedEntities;
}
