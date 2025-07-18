import React, { useEffect } from 'react';

import useDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useDomains';
import useTreeNodesFromDomains from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useTreeNodesFromListDomains';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

interface RequestProps {
    parentDomainUrn: string;
    onResponse: (nodes: TreeNode[], parentUrn: string) => void;
}

function AsyncDomainChildrenLoaderRequest({ parentDomainUrn, onResponse }: RequestProps) {
    const { domains } = useDomains(parentDomainUrn);
    const nodes = useTreeNodesFromDomains(domains);

    useEffect(() => {
        if (domains !== undefined) {
            onResponse(nodes, parentDomainUrn);
        }
    }, [domains, nodes, parentDomainUrn, onResponse]);

    return null;
}

interface Props {
    parentDomainsUrns: string[];
    onResponse: (nodes: TreeNode[], parentUrn: string) => void;
}

export default function AsyncDomainChildrenLoader({ parentDomainsUrns, onResponse }: Props) {
    return (
        <>
            {parentDomainsUrns.map((parentDomainsUrn) => (
                <AsyncDomainChildrenLoaderRequest
                    parentDomainUrn={parentDomainsUrn}
                    onResponse={onResponse}
                    key={parentDomainsUrn}
                />
            ))}
        </>
    );
}
