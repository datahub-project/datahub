import React, { useEffect } from 'react';
import styled from 'styled-components/macro';
import { useGetRootGlossaryNodesQuery, useGetRootGlossaryTermsQuery } from '../../../graphql/glossary.generated';
import { GlossaryNode, GlossaryTerm } from '../../../types.generated';
import NodeItem from './NodeItem';
import TermItem from './TermItem';

const BrowserWrapper = styled.div`
    color: #262626;
    font-size: 12px;
    max-height: calc(100% - 47px);
    padding: 10px 20px 20px 20px;
    overflow: auto;
`;

interface Props {
    rootNodes?: GlossaryNode[];
    rootTerms?: GlossaryTerm[];
    isSelecting?: boolean;
    hideTerms?: boolean;
    openToEntity?: boolean;
    refreshBrowser?: boolean;
    nodeUrnToHide?: string;
    selectTerm?: (urn: string, displayName: string) => void;
    selectNode?: (urn: string, displayName: string) => void;
}

function GlossaryBrowser(props: Props) {
    const {
        rootNodes,
        rootTerms,
        isSelecting,
        hideTerms,
        refreshBrowser,
        openToEntity,
        nodeUrnToHide,
        selectTerm,
        selectNode,
    } = props;

    const { data: nodesData, refetch: refetchNodes } = useGetRootGlossaryNodesQuery({ skip: !!rootNodes });
    const { data: termsData, refetch: refetchTerms } = useGetRootGlossaryTermsQuery({ skip: !!rootTerms });

    const displayedNodes = rootNodes || nodesData?.getRootGlossaryNodes?.nodes || [];
    const displayedTerms = rootTerms || termsData?.getRootGlossaryTerms?.terms || [];

    useEffect(() => {
        if (refreshBrowser) {
            refetchNodes();
            refetchTerms();
        }
    }, [refreshBrowser, refetchNodes, refetchTerms]);

    return (
        <BrowserWrapper>
            {displayedNodes.map((node) => (
                <NodeItem
                    key={node.urn}
                    node={node}
                    isSelecting={isSelecting}
                    hideTerms={hideTerms}
                    openToEntity={openToEntity}
                    refreshBrowser={refreshBrowser}
                    nodeUrnToHide={nodeUrnToHide}
                    selectTerm={selectTerm}
                    selectNode={selectNode}
                />
            ))}
            {!hideTerms &&
                displayedTerms.map((term) => (
                    <TermItem key={term.urn} term={term} isSelecting={isSelecting} selectTerm={selectTerm} />
                ))}
        </BrowserWrapper>
    );
}

export default GlossaryBrowser;
