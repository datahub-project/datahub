import React, { useEffect } from 'react';

import useGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useGlossaryNodesAndTerms';
import useTreeNodesFromGlossaryNodesAndTerms from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useTreeNodesFromGlossaryNodesAndTerms';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

interface RequestProps {
    glossaryNodeUrn: string;
    onResponse: (nodes: TreeNode[], parentUrn: string) => void;
}

function AsyncGlossaryNodeChildrenLoaderRequest({ glossaryNodeUrn, onResponse }: RequestProps) {
    const { glossaryNodes, glossaryTerms } = useGlossaryNodesAndTerms({ parentGlossaryNodeUrn: glossaryNodeUrn });
    const { treeNodes } = useTreeNodesFromGlossaryNodesAndTerms(glossaryNodes, glossaryTerms);

    useEffect(() => {
        if (glossaryNodes !== undefined && glossaryTerms !== undefined) {
            onResponse(treeNodes, glossaryNodeUrn);
        }
    }, [glossaryNodes, glossaryTerms, treeNodes, glossaryNodeUrn, onResponse]);

    return null;
}

interface Props {
    parentGlossaryNodesUrns: string[];
    onResponse: (nodes: TreeNode[], parentUrn: string) => void;
}

export default function AsyncGlossaryNodeChildrenLoader({ parentGlossaryNodesUrns, onResponse }: Props) {
    return (
        <>
            {parentGlossaryNodesUrns.map((parentGlossaryNodeUrn) => (
                <AsyncGlossaryNodeChildrenLoaderRequest
                    glossaryNodeUrn={parentGlossaryNodeUrn}
                    onResponse={onResponse}
                    key={parentGlossaryNodeUrn}
                />
            ))}
        </>
    );
}
