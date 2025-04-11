import { useEntityData } from '@app/entity/shared/EntityContext';
import merge from 'deepmerge';
import { keyBy, unionBy, values } from 'lodash';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router-dom';
import {
    Dataset,
    Entity,
    Health,
    HealthStatus,
    HealthStatusType,
    Maybe,
    ScrollResults,
    Operation,
    SiblingProperties,
} from '../../../types.generated';
import { GenericEntityProperties } from './types';
import { useIsShowSeparateSiblingsEnabled } from '../../useAppConfig';
import { downgradeV2FieldPath } from '../dataset/profile/schema/utils/utils';

export function stripSiblingsFromEntity(entity: any) {
    return {
        ...entity,
        siblings: null,
        siblingsSearch: null,
        siblingPlatforms: null,
    };
}

function cleanHelper(obj, visited) {
    if (visited.has(obj)) return obj;
    visited.add(obj);

    const object = obj;
    Object.entries(object).forEach(([k, v]) => {
        if (v && typeof v === 'object') {
            cleanHelper(v, visited);
        }
        if ((v && typeof v === 'object' && !Object.keys(v).length) || v === null || v === undefined || v === '') {
            if (Array.isArray(object)) {
                // do nothing
            } else if (Object.getOwnPropertyDescriptor(object, k)?.configurable) {
                // TODO(hsheth2): Not sure why we needed to add the above "configurable" check.
                // However, I was getting errors when it was not present in dev mode (but not in prod mode).
                try {
                    delete object[k];
                } catch (e) {
                    console.warn('error deleting key', k, 'from object', object, e);
                }
            }
        }
    });
    return object;
}

function clean(obj) {
    const visited = new Set();
    return cleanHelper(obj, visited);
}

const combineMerge = (target, source, options) => {
    const destination = target.slice();

    source.forEach((item, index) => {
        if (typeof destination[index] === 'undefined') {
            destination[index] = options.cloneUnlessOtherwiseSpecified(item, options);
        } else if (options.isMergeableObject(item)) {
            destination[index] = merge(target[index], item, options);
        } else if (target.indexOf(item) === -1) {
            destination.push(item);
        }
    });
    return destination;
};

// this function is responsible for normalizing object keys to make sure merging on key matches keys appropriately
function normalizeObjectKeys(object: Record<string, unknown>, isSchemaField = false) {
    return Object.fromEntries(
        Object.entries(object).map(([key, value]) => {
            let normalizedKey = key.toLowerCase();
            if (isSchemaField) {
                normalizedKey = downgradeV2FieldPath(normalizedKey) || normalizedKey;
            }
            return [normalizedKey, value];
        }),
    );
}

// use when you want to merge an array of objects by key in the object as opposed to by index of array
const mergeArrayOfObjectsByKey = (destinationArray: any[], sourceArray: any[], key: string, isSchemaField = false) => {
    const destination = normalizeObjectKeys(keyBy(destinationArray, key), isSchemaField);
    const source = normalizeObjectKeys(keyBy(sourceArray, key), isSchemaField);

    return values(
        merge(destination, source, {
            arrayMerge: combineMerge,
            customMerge,
        }),
    );
};

const mergeTags = (destinationArray, sourceArray, _options) => {
    return unionBy(destinationArray, sourceArray, 'tag.urn');
};

const mergeTerms = (destinationArray, sourceArray, _options) => {
    return unionBy(destinationArray, sourceArray, 'term.urn');
};

const mergeAssertions = (destinationArray, sourceArray, _options) => {
    return unionBy(destinationArray, sourceArray, 'urn');
};

const mergeIncidents = (destinationArray, sourceArray, _options) => {
    return unionBy(destinationArray, sourceArray, 'urn');
};

const mergeProperties = (destinationArray, sourceArray, _options) => {
    return unionBy(destinationArray, sourceArray, 'key');
};

const mergeStructuredProperties = (destinationArray, sourceArray, _options) => {
    return unionBy(sourceArray, destinationArray, 'structuredProperty.urn');
};

const mergeOwners = (destinationArray, sourceArray, _options) => {
    return unionBy(destinationArray, sourceArray, 'owner.urn');
};

const mergeFields = (destinationArray, sourceArray, _options) => {
    return mergeArrayOfObjectsByKey(destinationArray, sourceArray, 'fieldPath', true);
};

const mergeForms = (destinationArray, sourceArray, _options) => {
    return unionBy(sourceArray, destinationArray, 'form.urn');
};

const mergeLastOperations = (
    destinationArray: Pick<Operation, 'lastUpdatedTimestamp'>[],
    sourceArray: Pick<Operation, 'lastUpdatedTimestamp'>[],
    _options,
) => {
    // return whichever operation is more recent
    // const lastUpdated = (operations?.length && operations[0].lastUpdatedTimestamp) || 0;
    const destinationLastUpdated = (destinationArray?.length && destinationArray[0]?.lastUpdatedTimestamp) || 0;
    const sourceLastUpdated = (sourceArray?.length && sourceArray[0]?.lastUpdatedTimestamp) || 0;
    // return whichever operation is more recent
    return destinationLastUpdated > sourceLastUpdated ? destinationArray : sourceArray;
};

const mergeSubtypes = (destinationArray: string[], sourceArray: string[], _options) => {
    const seen = new Set<string>();
    const result: string[] = [];
    [...sourceArray, ...destinationArray].forEach((subtype) => {
        if (!seen.has(subtype)) {
            seen.add(subtype);
            result.push(subtype);
        }
    });
    return result;
};

const mergeHealthStatus = (destStatus?: HealthStatus, sourceStatus?: HealthStatus): HealthStatus => {
    if (destStatus === HealthStatus.Fail || sourceStatus === HealthStatus.Fail) {
        return HealthStatus.Fail;
    }
    if (destStatus === HealthStatus.Warn || sourceStatus === HealthStatus.Warn) {
        return HealthStatus.Warn;
    }
    return HealthStatus.Pass;
};

const mergeHealthMessage = (type: HealthStatusType, mergedStatus: HealthStatus): string => {
    if (mergedStatus === HealthStatus.Fail) {
        switch (type) {
            case HealthStatusType.Assertions:
                return 'See failing assertions →';
            case HealthStatusType.Incidents:
                return 'See active incidents →';
            default:
                return 'See failed checks →';
        }
    }
    if (mergedStatus === HealthStatus.Warn) {
        switch (type) {
            case HealthStatusType.Assertions:
                return 'Some assertions have problems.';
            default:
                return 'Some checks have problems.';
        }
    }
    if (mergedStatus === HealthStatus.Pass) {
        switch (type) {
            case HealthStatusType.Assertions:
                return 'All assertions are passing';
            case HealthStatusType.Incidents:
                return 'No active incidents';
            default:
                return 'All checks are passing';
        }
    }
    return 'All checks are passing';
};

// Merge entity health across siblings.
const mergeHealth = (
    destinationArray: Maybe<Health[]> | undefined,
    sourceArray: Maybe<Health[]> | undefined,
    _options,
) => {
    const viewedHealthType = new Set();
    return [...(sourceArray || []), ...(destinationArray || [])]
        .map((source) => {
            if (viewedHealthType.has(source.type)) {
                return null;
            }

            viewedHealthType.add(source.type);

            const { type, status, causes } = source;

            const destHealth = destinationArray?.find((dest) => dest.type === type);
            const destStatus = destHealth?.status;
            const destCauses = destHealth?.causes;

            const finalStatus = mergeHealthStatus(destStatus, status);
            const finalMessage = mergeHealthMessage(type, finalStatus);
            const finalCauses = [...(causes || []), ...(destCauses || [])];

            return {
                type,
                status: finalStatus,
                message: finalMessage,
                causes: finalCauses,
            };
        })
        .filter((health) => health !== null);
};

function getArrayMergeFunction(key) {
    switch (key) {
        case 'tags':
            return mergeTags;
        case 'terms':
            return mergeTerms;
        case 'assertions':
            return mergeAssertions;
        case 'customProperties':
            return mergeProperties;
        case 'owners':
            return mergeOwners;
        case 'incidents':
            return mergeIncidents;
        case 'fields':
            return mergeFields;
        case 'editableSchemaFieldInfo':
            return mergeFields;
        case 'health':
            return mergeHealth;
        case 'typeNames':
            return mergeSubtypes;
        case 'lastOperation':
            return mergeLastOperations;
        case 'completedForms':
        case 'incompleteForms':
        case 'verifications':
            return mergeForms;
        default:
            return undefined;
    }
}

// needs its own merge function because "properties" exists as a key elsewhere
function structuredPropertiesMerge(isPrimary, key) {
    if (key === 'properties') {
        return (secondary, primary) => {
            return merge(secondary, primary, {
                arrayMerge: mergeStructuredProperties,
                customMerge: customMerge.bind({}, isPrimary),
            });
        };
    }
    return (secondary, primary) => {
        return merge(secondary, primary, {
            arrayMerge: combineMerge,
            customMerge: customMerge.bind({}, isPrimary),
        });
    };
}

function customMerge(isPrimary, key) {
    if (key === 'upstream' || key === 'downstream') {
        return (_secondary, primary) => primary;
    }
    // take the platform & siblings of whichever entity we're merging with, rather than the primary
    if (key === 'platform' || key === 'siblings') {
        return (secondary, primary) => (isPrimary ? primary : secondary);
    }
    if (key === 'testResults') {
        return (_secondary, primary) => primary;
    }
    if (key === 'activeIncidents') {
        return (secondary, primary) => ({ ...primary, total: primary.total + secondary.total });
    }
    if (key === 'lastModified') {
        return (secondary, primary) => (secondary?.time || primary?.time < 0 || 0 ? secondary : primary);
    }
    if (key === 'statsSummary') {
        return (secondary, primary) => {
            if (!primary) {
                return secondary;
            }
            if (!secondary) {
                return primary;
            }
            return {
                ...primary,
                queryCountLast30Days: primary?.queryCountLast30Days || secondary?.queryCountLast30Days,
                queryCountPercentileLast30Days:
                    primary?.queryCountPercentileLast30Days || secondary?.queryCountPercentileLast30Days,
                uniqueUserCountLast30Days: primary?.uniqueUserCountLast30Days || secondary?.uniqueUserCountLast30Days,
                uniqueUserPercentileLast30Days:
                    primary?.uniqueUserPercentileLast30Days || secondary?.uniqueUserPercentileLast30Days,
            };
        };
    }
    if (key === 'structuredProperties') {
        return (secondary, primary) => {
            return merge(secondary, primary, {
                arrayMerge: combineMerge,
                customMerge: structuredPropertiesMerge.bind({}, isPrimary),
            });
        };
    }
    if (
        key === 'tags' ||
        key === 'terms' ||
        key === 'assertions' ||
        key === 'customProperties' ||
        key === 'owners' ||
        key === 'incidents' ||
        key === 'fields' ||
        key === 'editableSchemaFieldInfo' ||
        key === 'health' ||
        key === 'typeNames' ||
        key === 'completedForms' ||
        key === 'incompleteForms' ||
        key === 'verifications'
    ) {
        return (secondary, primary) => {
            return merge(secondary, primary, {
                arrayMerge: getArrayMergeFunction(key),
                customMerge: customMerge.bind({}, isPrimary),
            });
        };
    }
    return (secondary, primary) => {
        return merge(secondary, primary, {
            arrayMerge: combineMerge,
            customMerge: customMerge.bind({}, isPrimary),
        });
    };
}

export const getEntitySiblingData = <T>(baseEntity: T): Maybe<SiblingProperties> => {
    if (!baseEntity) {
        return null;
    }
    const baseEntityKey = Object.keys(baseEntity)[0];
    const extractedBaseEntity = baseEntity[baseEntityKey];

    // eslint-disable-next-line @typescript-eslint/dot-notation
    return extractedBaseEntity?.['siblings'];
};

// should the entity's metadata win out against its siblings?
export const shouldEntityBeTreatedAsPrimary = (extractedBaseEntity: {
    siblings?: SiblingProperties | null;
    siblingsSearch?: ScrollResults | null;
}) => {
    const siblingsList = extractedBaseEntity?.siblingsSearch?.searchResults?.map((r) => r.entity) || [];

    // if the entity is marked as primary, take its metadata first
    const isPrimarySibling = !!extractedBaseEntity?.siblings?.isPrimary;

    // if no entity in the cohort is primary, just have the entity whos urn is navigated
    // to be primary
    const hasAnyPrimarySibling = siblingsList.find((sibling) => !!(sibling as any)?.siblings?.isPrimary) !== undefined;

    const isPrimary = isPrimarySibling || !hasAnyPrimarySibling;

    return isPrimary;
};

const combineEntityWithSiblings = (entity: GenericEntityProperties) => {
    const siblings = entity.siblingsSearch?.searchResults?.map((r) => r.entity) || [];

    if (!entity?.siblingsSearch?.count || !siblings.length) {
        return entity;
    }

    const isPrimary = shouldEntityBeTreatedAsPrimary(entity);

    const combinedBaseEntity: any = siblings.reduce(
        (prev, current) =>
            merge(clean(isPrimary ? current : prev), clean(isPrimary ? prev : current), {
                arrayMerge: combineMerge,
                customMerge: customMerge.bind({}, isPrimary),
            }),
        entity,
    );

    // if a key is null in the primary sibling, it will not merge with the secondary even if the secondary is not null
    const secondarySibling = isPrimary ? siblings[0] : entity;
    Object.keys(secondarySibling).forEach((key) => {
        if (combinedBaseEntity[key] === null && secondarySibling[key] !== null) {
            combinedBaseEntity[key] = secondarySibling[key];
        }
    });

    // Force the urn of the combined entity to the current entity urn.
    combinedBaseEntity.urn = entity.urn;

    combinedBaseEntity.properties = {
        ...combinedBaseEntity.properties,
        externalUrl: entity?.properties?.externalUrl,
    };

    return combinedBaseEntity;
};

export function combineEntityData<T>(entityValue: T, siblingValue: T, isPrimary: boolean) {
    if (!entityValue) return siblingValue;
    if (!siblingValue) return entityValue;

    return merge(clean(isPrimary ? siblingValue : entityValue), clean(isPrimary ? entityValue : siblingValue), {
        arrayMerge: combineMerge,
        customMerge: customMerge.bind({}, isPrimary),
    });
}

export const combineEntityDataWithSiblings = <T>(baseEntity: T): T => {
    if (!baseEntity) {
        return baseEntity;
    }
    const baseEntityKey = Object.keys(baseEntity)[0];
    const extractedBaseEntity = baseEntity[baseEntityKey];

    if (!extractedBaseEntity?.siblingsSearch?.count) {
        return baseEntity;
    }

    const combinedBaseEntity = combineEntityWithSiblings(extractedBaseEntity);

    return { [baseEntityKey]: combinedBaseEntity } as unknown as T;
};

export type CombinedEntity = {
    entity: Entity;
    matchedEntities?: Array<Entity>;
};

type CombinedEntityResult =
    | {
          skipped: true;
      }
    | {
          skipped: false;
          combinedEntity: CombinedEntity;
      };

export function combineSiblingsForEntity(entity: Entity, visitedSiblingUrns: Set<string>): CombinedEntityResult {
    if (visitedSiblingUrns.has(entity.urn)) return { skipped: true };

    const combinedEntity: CombinedEntity = { entity: combineEntityWithSiblings({ ...entity }) };
    const siblings =
        (combinedEntity.entity as GenericEntityProperties).siblingsSearch?.searchResults?.map((r) => r.entity) ?? [];
    const isPrimary = (combinedEntity.entity as GenericEntityProperties).siblings?.isPrimary;
    const siblingUrns = siblings.map((sibling) => sibling?.urn);

    if (siblingUrns.length > 0) {
        combinedEntity.matchedEntities = isPrimary
            ? [stripSiblingsFromEntity(combinedEntity.entity), ...siblings]
            : [...siblings, stripSiblingsFromEntity(combinedEntity.entity)];

        combinedEntity.matchedEntities = combinedEntity.matchedEntities.filter(
            (resultToFilter) => (resultToFilter as Dataset).exists,
        );

        siblingUrns.forEach((urn) => urn && visitedSiblingUrns.add(urn));
    }

    return { combinedEntity, skipped: false };
}

export function createSiblingEntityCombiner() {
    const visitedSiblingUrns: Set<string> = new Set();
    return (entity: Entity) => combineSiblingsForEntity(entity, visitedSiblingUrns);
}

// used to determine whether sibling entities should be shown merged or not
export const SEPARATE_SIBLINGS_URL_PARAM = 'separate_siblings';

// used to determine whether sibling entities should be shown merged or not
export function useIsSeparateSiblingsMode() {
    const showSeparateSiblings = useIsShowSeparateSiblingsEnabled();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });

    return showSeparateSiblings || params[SEPARATE_SIBLINGS_URL_PARAM] === 'true';
}

/**
 * `siblingPlatforms` in GenericEntityProperties always puts the primary first.
 * This method allows getting sibling platforms without considering the primary.
 */
export function useGetSiblingPlatforms() {
    const { entityData } = useEntityData();
    const isPrimary = entityData?.siblings?.isPrimary ?? false;
    return {
        entityPlatform: isPrimary ? entityData?.siblingPlatforms?.[0] : entityData?.siblingPlatforms?.[1],
        siblingPlatform: isPrimary ? entityData?.siblingPlatforms?.[1] : entityData?.siblingPlatforms?.[0],
    };
}
