import { Plus } from 'phosphor-react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import CreateGlossaryEntityModal from '@app/entity/shared/EntityDropdown/CreateGlossaryEntityModal';
import useGlossaryChildren from '@app/entityV2/glossaryNode/useGlossaryChildren';
import { sortGlossaryNodes } from '@app/entityV2/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entityV2/glossaryTerm/utils';
import EmptyGlossarySection from '@app/glossaryV2/EmptyGlossarySection';
import GlossaryEntitiesList from '@app/glossaryV2/GlossaryEntitiesList';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Button, SearchBar, Tooltip } from '@src/alchemy-components';
import Loading from '@src/app/shared/Loading';

import { EntityType, GlossaryNode, GlossaryTerm } from '@types';

const ChildrenTabWrapper = styled.div`
    height: 100%;
    overflow: auto;
    padding-bottom: 10px;
`;

const LoadingWrapper = styled.div`
    height: 100px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

const StyledPlusOutlined = styled(Plus)`
    font-size: 12px;
`;

const CreateButtonWrapper = styled.div`
    display: flex;
    gap: 8px;
    margin-left: 16px;
    flex-direction: row;
`;

const HeaderWrapper = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
    margin: 16px;
`;

function ChildrenTab() {
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const entityUrn = entityData?.urn;

    const { scrollRef, data, loading, searchQuery, setSearchQuery, refetch } = useGlossaryChildren({ entityUrn });

    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);
    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);

    if (!entityData) return <></>;

    const childNodes = data
        .filter((child) => child.type === EntityType.GlossaryNode)
        .sort((nodeA, nodeB) => sortGlossaryNodes(entityRegistry, nodeA, nodeB));
    const childTerms = data
        .filter((child) => child.type === EntityType.GlossaryTerm)
        .sort((termA, termB) => sortGlossaryTerms(entityRegistry, termA, termB));

    const hasTermsOrNodes = !!childNodes?.length || !!childTerms?.length;

    if (searchQuery || hasTermsOrNodes) {
        return (
            <ChildrenTabWrapper>
                <HeaderWrapper>
                    <SearchBar
                        placeholder="Search..."
                        onChange={setSearchQuery}
                        value={searchQuery}
                        allowClear
                        width={hasTermsOrNodes ? 'auto' : '300px'}
                    />
                    {hasTermsOrNodes && (
                        <CreateButtonWrapper>
                            <Tooltip title="Create New Glossary Term" showArrow={false} placement="bottom">
                                <Button data-testid="add-term-button" onClick={() => setIsCreateTermModalVisible(true)}>
                                    <StyledPlusOutlined /> Add Term
                                </Button>
                            </Tooltip>
                            <Tooltip title="Create New Term Group" showArrow={false} placement="bottom">
                                <Button
                                    data-testid="add-term-group-button-v2"
                                    variant="outline"
                                    onClick={() => setIsCreateNodeModalVisible(true)}
                                >
                                    <StyledPlusOutlined /> Add Term Group
                                </Button>
                            </Tooltip>
                        </CreateButtonWrapper>
                    )}
                </HeaderWrapper>
                <GlossaryEntitiesList
                    nodes={(childNodes as GlossaryNode[]) || []}
                    terms={(childTerms as GlossaryTerm[]) || []}
                />
                {loading && (
                    <LoadingWrapper>
                        <Loading marginTop={0} height={24} />
                    </LoadingWrapper>
                )}
                <div ref={scrollRef} />

                {isCreateTermModalVisible && (
                    <CreateGlossaryEntityModal
                        entityType={EntityType.GlossaryTerm}
                        onClose={() => setIsCreateTermModalVisible(false)}
                        refetchData={refetch}
                    />
                )}
                {isCreateNodeModalVisible && (
                    <CreateGlossaryEntityModal
                        entityType={EntityType.GlossaryNode}
                        onClose={() => setIsCreateNodeModalVisible(false)}
                        refetchData={refetch}
                    />
                )}
            </ChildrenTabWrapper>
        );
    }

    return <EmptyGlossarySection description="No Terms or Term Groups" />;
}

export default ChildrenTab;
