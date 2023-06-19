import React, { useEffect } from 'react';
import styled from 'styled-components/macro';
import { useGetRootGlossaryNodesQuery, useGetRootGlossaryTermsQuery } from '../../../graphql/glossary.generated';
import { ChildGlossaryTermFragment } from '../../../graphql/glossaryNode.generated';
import { GlossaryNode } from '../../../types.generated';
import { sortGlossaryNodes } from '../../entity/glossaryNode/utils';
import { sortGlossaryTerms } from '../../entity/glossaryTerm/utils';
import { useGlossaryEntityData } from '../../entity/shared/GlossaryEntityContext';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ROOT_NODES, ROOT_TERMS } from '../utils';
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
    rootTerms?: ChildGlossaryTermFragment[];
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

    const { urnsToUpdate, setUrnsToUpdate } = useGlossaryEntityData();

    const { data: nodesData, refetch: refetchNodes } = useGetRootGlossaryNodesQuery({ skip: !!rootNodes });
    const { data: termsData, refetch: refetchTerms } = useGetRootGlossaryTermsQuery({ skip: !!rootTerms });

    const displayedNodes = rootNodes || nodesData?.getRootGlossaryNodes?.nodes || [];
    const displayedTerms = rootTerms || termsData?.getRootGlossaryTerms?.terms || [];

    const entityRegistry = useEntityRegistry();
    const sortedNodes = displayedNodes.sort((nodeA, nodeB) => sortGlossaryNodes(entityRegistry, nodeA, nodeB));
    const sortedTerms = displayedTerms.sort((termA, termB) => sortGlossaryTerms(entityRegistry, termA, termB));

    useEffect(() => {
        if (refreshBrowser) {
            refetchNodes();
            refetchTerms();
        }
    }, [refreshBrowser, refetchNodes, refetchTerms]);

    // if node(s) or term(s) need to be refreshed at the root level, check if these special cases are in `urnsToUpdate`
    useEffect(() => {
        if (urnsToUpdate.includes(ROOT_NODES)) {
            refetchNodes();
            setUrnsToUpdate(urnsToUpdate.filter((urn) => urn !== ROOT_NODES));
        }
        if (urnsToUpdate.includes(ROOT_TERMS)) {
            refetchTerms();
            setUrnsToUpdate(urnsToUpdate.filter((urn) => urn !== ROOT_TERMS));
        }
    });

    return (
        <BrowserWrapper>
            {sortedNodes.map((node) => (
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
                sortedTerms.map((term) => (
                    <TermItem key={term.urn} term={term} isSelecting={isSelecting} selectTerm={selectTerm} />
                ))}
        </BrowserWrapper>
    );
}

export default GlossaryBrowser;
