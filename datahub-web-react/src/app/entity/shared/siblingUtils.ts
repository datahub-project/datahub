import { Dataset } from '../../../types.generated';
import { GenericEntityProperties } from './types';

const FIELDS_TO_EXCLUDE_FROM_SIBLINGS = ['container'];

export const omitEmpty = (obj: any, isSibling: boolean) => {
    if (!obj) {
        return obj;
    }

    const result = {};
    Object.keys(obj).forEach((key) => {
        if (isSibling && FIELDS_TO_EXCLUDE_FROM_SIBLINGS.indexOf(key) > -1) {
            return;
        }

        const val = obj[key];
        // skip nulls, empty arrays and also empty groups of buckets, for timeseries aspects
        // lineage is already taken care of in getLineageVizConfig logic
        if (val !== null && val !== undefined && val?.length !== 0 && val?.buckets?.length !== 0 && val?.total !== 0) {
            result[key] = val;
        }
    });

    return result;
};

export const getPrimarySiblingFromEntity = (entity: Dataset) => {
    if (!entity.siblings || entity.siblings?.isPrimary) {
        return entity;
    }

    const primarySibling = entity.siblings?.siblings?.filter((sibling) => {
        if (sibling) {
            return (sibling as Dataset).siblings?.isPrimary;
        }
        return false;
    })[0];

    if (primarySibling) {
        return primarySibling;
    }

    return entity;
};

export const getUpstreamsAndDownstreamsFromEntityAndSiblings = (entity: GenericEntityProperties | null) => {
    if (!entity) {
        return { allUpstreams: [], allDownstreams: [] };
    }

    // logic for handling lineage is special because we want to merge the entity with its siblings lineage
    let siblingsUpstreams: any[] = [];
    let siblingsDownstreams: any[] = [];

    // eslint-disable-next-line @typescript-eslint/dot-notation
    let datasetDownstreamsWithoutSiblings = entity?.['downstream']?.relationships || [];
    // eslint-disable-next-line @typescript-eslint/dot-notation
    let datasetUpstreamsWithoutSiblings = entity?.['upstream']?.relationships || [];

    if (entity.siblings) {
        if (entity.siblings.isPrimary) {
            const siblingUrns = entity.siblings.siblings?.map((sibling) => sibling?.urn);
            datasetDownstreamsWithoutSiblings = datasetDownstreamsWithoutSiblings.filter(
                (downstream) => !siblingUrns?.includes(downstream?.entity?.urn),
            );
            datasetUpstreamsWithoutSiblings = datasetUpstreamsWithoutSiblings.filter(
                (upstream) => !siblingUrns?.includes(upstream?.entity?.urn),
            );
            siblingsUpstreams =
                entity.siblings.siblings
                    // eslint-disable-next-line @typescript-eslint/dot-notation
                    ?.flatMap((sibling) => sibling?.['upstream']?.relationships || [])
                    .filter((relationship) => relationship.entity.urn !== entity.urn) || [];
            siblingsDownstreams =
                entity.siblings.siblings
                    // eslint-disable-next-line @typescript-eslint/dot-notation
                    ?.flatMap((sibling) => sibling?.['downstream']?.relationships || [])
                    .filter((relationship) => relationship.entity.urn !== entity.urn) || [];
        }
    }

    const allDownstreams = [...siblingsDownstreams, ...datasetDownstreamsWithoutSiblings]
        ?.filter((relationship) => relationship.entity)
        .map((relationship) => relationship.entity)
        .map((relatedEntity) => getPrimarySiblingFromEntity(relatedEntity));

    const allUpstreams = [...siblingsUpstreams, ...datasetUpstreamsWithoutSiblings]
        ?.filter((relationship) => relationship.entity)
        .map((relationship) => relationship.entity)
        .map((relatedEntity) => getPrimarySiblingFromEntity(relatedEntity));

    return { allUpstreams, allDownstreams };
};

export const combineEntityDataWithSiblings = <T>(baseEntity: T): T => {
    if (!baseEntity) {
        return baseEntity;
    }
    const baseEntityKey = Object.keys(baseEntity)[0];
    const extractedBaseEntity = baseEntity[baseEntityKey];

    // eslint-disable-next-line @typescript-eslint/dot-notation
    if ((extractedBaseEntity?.['siblings']?.siblings || []).length === 0) {
        return baseEntity;
    }

    // eslint-disable-next-line @typescript-eslint/dot-notation
    const siblings: T[] = extractedBaseEntity?.['siblings']?.siblings || [];
    const combinedBaseEntity = siblings.reduce(
        (prev, current) => ({ ...omitEmpty(current, true), ...prev }),
        omitEmpty(extractedBaseEntity, false),
    ) as T;

    // eslint-disable-next-line no-param-reassign
    baseEntity[baseEntityKey] = combinedBaseEntity;

    return baseEntity;
};
