import merge from 'deepmerge';
import { unionBy } from 'lodash';
import { Entity, MatchedField, Maybe, SiblingProperties } from '../../../types.generated';

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
                object.splice(Number(k), 1);
            } else {
                delete object[k];
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

const mergeTags = (destinationArray, sourceArray, _options) => {
    return unionBy(destinationArray, sourceArray, 'tag.urn');
};

const mergeTerms = (destinationArray, sourceArray, _options) => {
    return unionBy(destinationArray, sourceArray, 'term.urn');
};

const mergeAssertions = (destinationArray, sourceArray, _options) => {
    return unionBy(destinationArray, sourceArray, 'urn');
};

function getArrayMergeFunction(key) {
    switch (key) {
        case 'tags':
            return mergeTags;
        case 'terms':
            return mergeTerms;
        case 'assertions':
            return mergeAssertions;
        default:
            return undefined;
    }
}

const customMerge = (isPrimary, key) => {
    if (key === 'upstream' || key === 'downstream') {
        return (_secondary, primary) => primary;
    }
    if (key === 'platform') {
        return (secondary, primary) => (isPrimary ? primary : secondary);
    }
    if (key === 'tags' || key === 'terms' || key === 'assertions') {
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
};

export const getEntitySiblingData = <T>(baseEntity: T): Maybe<SiblingProperties> => {
    if (!baseEntity) {
        return null;
    }
    const baseEntityKey = Object.keys(baseEntity)[0];
    const extractedBaseEntity = baseEntity[baseEntityKey];

    // eslint-disable-next-line @typescript-eslint/dot-notation
    return extractedBaseEntity?.['siblings'];
};

export const combineEntityDataWithSiblings = <T>(baseEntity: T): T => {
    if (!baseEntity) {
        return baseEntity;
    }
    const baseEntityKey = Object.keys(baseEntity)[0];
    const extractedBaseEntity = baseEntity[baseEntityKey];

    // eslint-disable-next-line @typescript-eslint/dot-notation
    const siblingAspect = extractedBaseEntity.siblings;
    if ((siblingAspect?.siblings || []).length === 0) {
        return baseEntity;
    }

    // eslint-disable-next-line @typescript-eslint/dot-notation
    const siblings: T[] = siblingAspect?.siblings || [];
    const isPrimary = !!extractedBaseEntity?.siblings?.isPrimary;

    const combinedBaseEntity: any = siblings.reduce(
        (prev, current) =>
            merge(clean(isPrimary ? current : prev), clean(isPrimary ? prev : current), {
                arrayMerge: combineMerge,
                customMerge: customMerge.bind({}, isPrimary),
            }),
        extractedBaseEntity,
    ) as T;

    // Force the urn of the combined entity to the current entity urn.
    combinedBaseEntity.urn = extractedBaseEntity.urn;

    return { [baseEntityKey]: combinedBaseEntity } as unknown as T;
};

export type CombinedSearchResult = {
    entity: Entity;
    matchedFields: MatchedField[];
    matchedEntities?: Entity[];
};

export function combineSiblingsInSearchResults(
    results:
        | {
              entity: Entity;
              matchedFields: MatchedField[];
          }[]
        | undefined,
) {
    const combinedResults: CombinedSearchResult[] | undefined = [];
    const siblingsToPair: Record<string, CombinedSearchResult> = {};

    // set sibling associations
    results?.forEach((result) => {
        if (result.entity.urn in siblingsToPair) {
            // filter from repeating
            // const siblingsCombinedResult = siblingsToPair[result.entity.urn];
            // siblingsCombinedResult.matchedEntities?.push(result.entity);
            return;
        }

        const combinedResult: CombinedSearchResult = result;
        const { entity }: { entity: any } = result;
        const siblingUrns = entity?.siblings?.siblings?.map((sibling) => sibling.urn) || [];
        if (siblingUrns.length > 0) {
            combinedResult.matchedEntities = entity.siblings.isPrimary
                ? [entity, ...entity.siblings.siblings]
                : [...entity.siblings.siblings, entity];
            siblingUrns.forEach((urn) => {
                siblingsToPair[urn] = combinedResult;
            });
        }
        combinedResults.push(combinedResult);
    });

    return combinedResults;
}
