import { useEffect } from 'react';

import { useDomainsContext } from '@app/domainV2/DomainsContext';

import { ListDomainFragment } from '@graphql/domain.generated';

interface Props {
    dataUrnsSet: Set<string>;
    setDataUrnsSet: React.Dispatch<React.SetStateAction<Set<string>>>;
    setData: React.Dispatch<React.SetStateAction<ListDomainFragment[]>>;
    parentDomain?: string;
}

export default function useManageDomains({ dataUrnsSet, setDataUrnsSet, setData, parentDomain }: Props) {
    const { newDomain, setNewDomain, deletedDomain, setDeletedDomain, updatedDomain, setUpdatedDomain } =
        useDomainsContext();

    // Adding new domain
    useEffect(() => {
        if (newDomain && newDomain.parentDomain === parentDomain) {
            setData((prevData) => [newDomain, ...prevData]);
            setDataUrnsSet((currSet) => new Set([...currSet, newDomain.urn]));
            setNewDomain(null);
        }

        // adding a new domain should increase the count of its parent
        const newDomainParentUrn = newDomain?.parentDomain;
        if (newDomainParentUrn && dataUrnsSet.has(newDomainParentUrn)) {
            setData((currData) =>
                currData.map((d) => {
                    if (d.urn === newDomainParentUrn) {
                        return { ...d, children: { total: (d.children?.total || 0) + 1 } };
                    }
                    return d;
                }),
            );
        }
    }, [newDomain, dataUrnsSet, parentDomain, setData, setDataUrnsSet, setNewDomain]);

    // Deleting domain
    useEffect(() => {
        if (deletedDomain && dataUrnsSet.has(deletedDomain.urn)) {
            setData((prevData) => prevData.filter((d) => d.urn !== deletedDomain.urn));
            setDeletedDomain(null);
        }

        // deleting a new domain should decrease the count of its parent
        const deletedDomainParentUrn = deletedDomain?.parentDomain;
        if (deletedDomainParentUrn && dataUrnsSet.has(deletedDomainParentUrn)) {
            setData((currData) =>
                currData.map((d) => {
                    if (d.urn === deletedDomainParentUrn) {
                        return { ...d, children: { total: Math.max((d.children?.total || 0) - 1, 0) } };
                    }
                    return d;
                }),
            );
        }
    }, [deletedDomain, dataUrnsSet, setData, setDeletedDomain]);

    // Updating domain
    useEffect(() => {
        if (updatedDomain && dataUrnsSet.has(updatedDomain.urn)) {
            setData((prevData) =>
                prevData.map((d) => {
                    if (d.urn === updatedDomain.urn) {
                        return { ...d, ...updatedDomain };
                    }
                    return d;
                }),
            );
            setUpdatedDomain(null);
        }
    }, [updatedDomain, dataUrnsSet, setData, setUpdatedDomain]);
}
