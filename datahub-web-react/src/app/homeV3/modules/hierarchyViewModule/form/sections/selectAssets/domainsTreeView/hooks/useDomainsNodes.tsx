import { useMemo } from 'react';

import { convertDomainToTreeNode } from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/utils';

import { ListDomainsQuery, useListDomainsQuery } from '@graphql/domain.generated';

type DomainsArray = NonNullable<ListDomainsQuery['listDomains']>['domains'];
export type DomainItem = DomainsArray extends Array<infer T> ? T : never;

interface Props {
    parentUrn?: string;
}

export default function useDomainsNodes({ parentUrn }: Props) {
    const { data, loading } = useListDomainsQuery({
        variables: {
            input: {
                start: 0,
                count: 1000,
                parentDomain: parentUrn,
            },
        },
    });

    const nodes = useMemo(() => (data?.listDomains?.domains ?? []).map(convertDomainToTreeNode), [data]);

    return { nodes, loading, data };
}
