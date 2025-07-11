import { useCallback, useMemo, useState } from 'react';

import useListDomains from '@app/domainV2/useListDomains';

import { ListDomainsQuery, useListDomainsLazyQuery } from '@graphql/domain.generated';
import { TreeNode } from './ItemsSelector';


type DomainsArray = NonNullable<ListDomainsQuery['listDomains']>['domains'];
type DomainItem = DomainsArray extends Array<infer T> ? T : never;

interface Props {
    onCompleted: (nodes: TreeNode[], parentDomainUrn?: string) => void;
}

const domainToTreeNode = (domain: DomainItem): TreeNode => {
    return {
        value: domain.urn,
        label: domain.urn,
        hasAsyncChildren: !!domain.children?.total,
    }
}

export default function useGetDomainOptions({ 
    onCompleted,
 }: Props) {

    const [parentDomainUrn, setParentDomainUrn] = useState<string | undefined>();

    // TODO: add sorting
    const onCompletedHandler = useCallback((data: ListDomainsQuery) => {
        const domains = data.listDomains?.domains ?? [];

        const nodes = domains.map(domainToTreeNode);
        onCompleted(nodes, parentDomainUrn);
        setParentDomainUrn(undefined);
    }, [onCompleted, parentDomainUrn])

    const [fetchDomains, { loading }] = useListDomainsLazyQuery({
        onCompleted: onCompletedHandler,
    });

    const getDomainOptions = useCallback((parentDomainUrnToFetchDomains?: string) => {
        setParentDomainUrn(parentDomainUrnToFetchDomains);
        fetchDomains({
            variables: {
                input: {
                    start: 0,
                    count: 1000,
                    parentDomain: parentDomainUrnToFetchDomains,
                },
            },
        })
    }, [fetchDomains])


    return { getDomainOptions, loading };
}
