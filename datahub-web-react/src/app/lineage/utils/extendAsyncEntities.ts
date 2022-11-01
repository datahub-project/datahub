import { SchemaFieldRef } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';
import { EntityAndType, FetchedEntities, FetchedEntity } from '../types';
import { getFieldPathFromSchemaFieldUrn, getSourceUrnFromSchemaFieldUrn } from './columnLineageUtils';

const breakFieldUrn = (ref: SchemaFieldRef) => {
    const before = ref.urn;
    const after = ref.path;

    return [before, after];
};

function updateFineGrainedMap(
    fineGrainedMap: any,
    upstreamEntityUrn: string,
    upstreamField: string,
    downstreamEntityUrn: string,
    downstreamField: string,
) {
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
}

function extendColumnLineage(lineageVizConfig: FetchedEntity, fineGrainedMap: any) {
    if (lineageVizConfig.fineGrainedLineages && lineageVizConfig.fineGrainedLineages.length > 0) {
        lineageVizConfig.fineGrainedLineages.forEach((fineGrainedLineage) => {
            fineGrainedLineage.upstreams?.forEach((upstream) => {
                const [upstreamEntityUrn, upstreamField] = breakFieldUrn(upstream);
                fineGrainedLineage.downstreams?.forEach((downstream) => {
                    const [downstreamEntityUrn, downstreamField] = breakFieldUrn(downstream);
                    updateFineGrainedMap(
                        fineGrainedMap,
                        upstreamEntityUrn,
                        upstreamField,
                        downstreamEntityUrn,
                        downstreamField,
                    );
                });
            });
        });
    }
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

    extendColumnLineage(lineageVizConfig, fineGrainedMap);

    return {
        ...fetchedEntities,
        [entityAndType.entity.urn]: {
            ...lineageVizConfig,
            fullyFetched,
        },
    };
}
