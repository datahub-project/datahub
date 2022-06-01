import { Dataset } from '../../../types.generated';

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
