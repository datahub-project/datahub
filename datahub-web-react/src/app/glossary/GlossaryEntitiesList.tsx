import React, { useState } from 'react';
import { Empty } from 'antd';
import { PlusOutlined } from '@ant-design/icons';

import styled from 'styled-components/macro';
import { GlossaryNodeFragment } from '../../graphql/fragments.generated';
import { EntityType, GlossaryNode, GlossaryTerm } from '../../types.generated';
import { useEntityRegistry } from '../useEntityRegistry';
import GlossaryEntityItem from './GlossaryEntityItem';
import StyledButton from '../entity/shared/components/styled/StyledButton';
import CreateGlossaryEntityModal from '../entity/shared/EntityDropdown/CreateGlossaryEntityModal';

const EntitiesWrapper = styled.div`
    flex: 1;
    overflow: auto;
    padding-bottom: 20px;
`;

const ButtonContainer = styled.div`
    display: flex;
    justify-content: center;
`;

interface Props {
    nodes: (GlossaryNode | GlossaryNodeFragment)[];
    terms: GlossaryTerm[];
    refetchForTerms?: () => void;
    refetchForNodes?: () => void;
}

function GlossaryEntitiesList(props: Props) {
    const { nodes, terms, refetchForTerms, refetchForNodes } = props;
    const entityRegistry = useEntityRegistry();
    const [isCreateTermModalVisible, setIsCreateTermModalVisible] = useState(false);
    const [isCreateNodeModalVisible, setIsCreateNodeModalVisible] = useState(false);

    const contentsData =
        nodes.length === 0 && terms.length === 0 ? (
            <>
                <Empty description="No Terms or Term Groups!" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                <ButtonContainer>
                    <StyledButton onClick={() => setIsCreateTermModalVisible(true)}>
                        <PlusOutlined /> Add Term
                    </StyledButton>
                    <StyledButton onClick={() => setIsCreateNodeModalVisible(true)} style={{ marginLeft: '10px' }}>
                        <PlusOutlined /> Add Term Group
                    </StyledButton>
                </ButtonContainer>

                {isCreateTermModalVisible && (
                    <CreateGlossaryEntityModal
                        entityType={EntityType.GlossaryTerm}
                        onClose={() => setIsCreateTermModalVisible(false)}
                        refetchData={refetchForTerms}
                    />
                )}
                {isCreateNodeModalVisible && (
                    <CreateGlossaryEntityModal
                        entityType={EntityType.GlossaryNode}
                        onClose={() => setIsCreateNodeModalVisible(false)}
                        refetchData={refetchForNodes}
                    />
                )}
            </>
        ) : (
            <EntitiesWrapper>
                {nodes.map((node) => (
                    <GlossaryEntityItem
                        name={node.properties?.name || ''}
                        urn={node.urn}
                        type={node.type}
                        count={(node as GlossaryNodeFragment).children?.count}
                    />
                ))}
                {terms.map((term) => (
                    <GlossaryEntityItem
                        name={entityRegistry.getDisplayName(term.type, term)}
                        urn={term.urn}
                        type={term.type}
                    />
                ))}
            </EntitiesWrapper>
        );
    return contentsData;
}

export default GlossaryEntitiesList;
