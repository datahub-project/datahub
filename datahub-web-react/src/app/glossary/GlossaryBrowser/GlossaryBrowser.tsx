import React from 'react';
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
    selectTerm?: (urn: string, displayName: string) => void;
    selectNode?: (urn: string, displayName: string) => void;
}

function GlossaryBrowser(props: Props) {
    const { rootNodes, rootTerms, isSelecting, hideTerms, refreshBrowser, openToEntity, selectTerm, selectNode } =
        props;

    const { data: nodesData } = useGetRootGlossaryNodesQuery({ skip: !!rootNodes });
    const { data: termsData } = useGetRootGlossaryTermsQuery({ skip: !!rootTerms });

    const displayedNodes = rootNodes || nodesData?.getRootGlossaryNodes?.nodes || [];
    const displayedTerms = rootTerms || termsData?.getRootGlossaryTerms?.terms || [];

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
