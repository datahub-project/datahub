import { ApolloClient } from '@apollo/client';
import { useEffect } from 'react';
import { isEqual } from 'lodash';
import { ListDomainsDocument, ListDomainsQuery } from '../../graphql/domain.generated';
import { Entity, EntityType } from '../../types.generated';
import { GenericEntityProperties } from '../entity/shared/types';
import usePrevious from '../shared/usePrevious';
import { useDomainsContext } from './DomainsContext';
import { useEntityRegistry } from '../useEntityRegistry';
import EntityRegistry from '../entity/EntityRegistry';

/**
 * Add an entry to the list domains cache.
 */
export const addToListDomainsCache = (client: ApolloClient<any>, newDomain, pageSize, parentDomain?: string) => {
    // Read the data from our cache for this query.
    const currData: ListDomainsQuery | null = client.readQuery({
        query: ListDomainsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
                parentDomain,
            },
        },
    });

    // Add our new domain into the existing list.
    const newDomains = [newDomain, ...(currData?.listDomains?.domains || [])];

    // Write our data back to the cache.
    client.writeQuery({
        query: ListDomainsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
                parentDomain,
            },
        },
        data: {
            listDomains: {
                start: 0,
                count: (currData?.listDomains?.count || 0) + 1,
                total: (currData?.listDomains?.total || 0) + 1,
                domains: newDomains,
            },
        },
    });
};

export const updateListDomainsCache = (
    client: ApolloClient<any>,
    urn: string,
    id: string | undefined,
    name: string,
    description: string | undefined,
    parentDomain?: string,
) => {
    addToListDomainsCache(
        client,
        {
            urn,
            id: id || '',
            type: EntityType.Domain,
            properties: {
                name,
                description: description || null,
            },
            ownership: null,
            entities: null,
            children: null,
            dataProducts: null,
            parentDomains: null,
            displayProperties: null,
        },
        1000,
        parentDomain,
    );
};

/**
 * Remove an entry from the list domains cache.
 */
export const removeFromListDomainsCache = (client, urn, page, pageSize, parentDomain?: string) => {
    // Read the data from our cache for this query.
    const currData: ListDomainsQuery | null = client.readQuery({
        query: ListDomainsDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                parentDomain,
            },
        },
    });

    // Remove the domain from the existing domain set.
    const newDomains = [...(currData?.listDomains?.domains || []).filter((domain) => domain.urn !== urn)];

    // Write our data back to the cache.
    client.writeQuery({
        query: ListDomainsDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                parentDomain,
            },
        },
        data: {
            listDomains: {
                start: currData?.listDomains?.start || 0,
                count: (currData?.listDomains?.count || 1) - 1,
                total: (currData?.listDomains?.total || 1) - 1,
                domains: newDomains,
            },
        },
    });
};

export function useUpdateDomainEntityDataOnChange(entityData: GenericEntityProperties | null, entityType: EntityType) {
    const { setEntityData } = useDomainsContext();
    const previousEntityData = usePrevious(entityData);

    useEffect(() => {
        if (EntityType.Domain === entityType && !isEqual(entityData, previousEntityData)) {
            setEntityData(entityData);
        }
    });
}

export function useSortedDomains<T extends Entity>(domains?: Array<T>, sortBy?: 'displayName') {
    const entityRegistry = useEntityRegistry();
    if (!domains || !sortBy) return domains;
    return [...domains].sort((a, b) => {
        const nameA = entityRegistry.getDisplayName(EntityType.Domain, a) || '';
        const nameB = entityRegistry.getDisplayName(EntityType.Domain, b) || '';
        return nameA.localeCompare(nameB);
    });
}

export function getParentDomains<T extends Entity>(domain: T, entityRegistry: EntityRegistry) {
    const props = entityRegistry.getGenericEntityProperties(EntityType.Domain, domain);
    return props?.parentDomains?.domains ?? [];
}
