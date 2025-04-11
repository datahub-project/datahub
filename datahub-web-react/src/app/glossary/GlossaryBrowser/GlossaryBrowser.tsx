import React, { useEffect } from 'react';
import styled from 'styled-components/macro';
import { LoadingOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '@src/app/entity/shared/constants';
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

const LoadingWrapper = styled.div`
    padding: 8px;
    display: flex;
    justify-content: center;

    svg {
        height: 15px;
        width: 15px;
        color: ${ANTD_GRAY[8]};
    }
`;

interface Props {
    rootNodes?: GlossaryNode[];
    rootTerms?: ChildGlossaryTermFragment[];
    isSelecting?: boolean;
    hideTerms?: boolean;
    openToEntity?: boolean;
    refreshBrowser?: boolean;
    nodeUrnToHide?: string;
    termUrnToHide?: string;
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
        termUrnToHide,
        selectTerm,
        selectNode,
    } = props;

    const { urnsToUpdate, setUrnsToUpdate } = useGlossaryEntityData();

    const {
        data: nodesData,
        refetch: refetchNodes,
        loading: nodesLoading,
    } = useGetRootGlossaryNodesQuery({ skip: !!rootNodes });
    const {
        data: termsData,
        refetch: refetchTerms,
        loading: termsLoading,
    } = useGetRootGlossaryTermsQuery({ skip: !!rootTerms });
    const loading = nodesLoading || termsLoading;

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
            {loading && (
                <LoadingWrapper>
                    <LoadingOutlined />
                </LoadingWrapper>
            )}
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
                    termUrnToHide={termUrnToHide}
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
