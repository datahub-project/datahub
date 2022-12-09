import { ListDomainsDocument, ListDomainsQuery } from '../../graphql/domain.generated';

/**
 * Add an entry to the list domains cache.
 */
export const addToListDomainsCache = (client, newDomain, pageSize, query) => {
    // Read the data from our cache for this query.
    const currData: ListDomainsQuery | null = client.readQuery({
        query: ListDomainsDocument,
        variables: {
            input: {
                start: 0,
                count: pageSize,
                query,
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
                query,
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

/**
 * Remove an entry from the list domains cache.
 */
export const removeFromListDomainsCache = (client, urn, page, pageSize, query) => {
    // Read the data from our cache for this query.
    const currData: ListDomainsQuery | null = client.readQuery({
        query: ListDomainsDocument,
        variables: {
            input: {
                start: (page - 1) * pageSize,
                count: pageSize,
                query,
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
                query,
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
