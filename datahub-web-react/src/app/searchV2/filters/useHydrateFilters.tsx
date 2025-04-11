import { useEffect, useMemo, useState } from 'react';
import { FieldType, FilterPredicate } from './types';
import { Entity } from '../../../types.generated';
import { useGetEntitiesLazyQuery } from '../../../graphql/entity.generated';
import { buildEntityCache, isResolutionRequired } from '../../entityV2/view/builder/utils';

export const useHydrateFilters = (filters: FilterPredicate[]): FilterPredicate[] => {
    // Stores an URN to the resolved entity.
    const [entityCache, setEntityCache] = useState<Map<string, Entity>>(new Map());

    // Find the filters requiring entity resolution.
    const filtersToResolve = useMemo(
        () => filters.filter((filter) => filter.field.type === FieldType.ENTITY) || [],
        [filters],
    );

    // Create an array of all urns requiring resolution
    const urnsToResolve: string[] = useMemo(() => {
        return filtersToResolve.flatMap((filter) => {
            return filter.values.filter((value) => !value.entity).map((value) => value.value);
        });
    }, [filtersToResolve]);

    /**
     * Bootstrap by resolving all URNs that are not in the cache yet.
     */
    const [getEntities, { data: resolvedEntitiesData }] = useGetEntitiesLazyQuery();

    useEffect(() => {
        if (isResolutionRequired(urnsToResolve, entityCache)) {
            getEntities({ variables: { urns: urnsToResolve } });
        }
    }, [urnsToResolve, entityCache, getEntities]);

    /**
     * If some entities need to be resolved, simply build the cache from them.
     *
     * This should only happen once at component bootstrap. Typically
     * all URNs will be missing from the cache.
     */
    useEffect(() => {
        if (resolvedEntitiesData && resolvedEntitiesData.entities?.length) {
            const entities: Entity[] = (resolvedEntitiesData?.entities as Entity[]) || [];
            setEntityCache(buildEntityCache(entities));
        }
    }, [resolvedEntitiesData]);

    /**
     * Replace the  with the resolved entity.
     */
    return useMemo(() => {
        return filters.map((filter) => {
            if (filter.field.type === FieldType.ENTITY) {
                return {
                    ...filter,
                    values: filter.values.map((value) => {
                        if (value.entity) {
                            return value;
                        }
                        const entity = entityCache.get(value.value);
                        return {
                            ...value,
                            entity: entity || null,
                        };
                    }),
                };
            }
            return filter;
        });
    }, [filters, entityCache]);
};
