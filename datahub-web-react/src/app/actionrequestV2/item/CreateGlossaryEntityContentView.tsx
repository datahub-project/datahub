import { colors, Text } from '@components';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { ActionRequest, EntityType, GlossaryNode } from '@src/types.generated';
import MDEditor from '@uiw/react-md-editor';
import { Modal } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { FileText } from 'phosphor-react';
import CreatedByView from './CreatedByView';
import { ViewDocumentationButton } from './updateDescription/UpdateDescriptionRequestItem';
import { ContentWrapper, StyledLink } from './styledComponents';

const NameWrapper = styled.span`
    font-weight: bold;
`;

interface Props {
    proposedName: string;
    actionRequest: ActionRequest;
    entityName: string;
    parentNode: GlossaryNode | null;
    description: string;
}

function CreateGlossaryEntityContentView({ proposedName, actionRequest, entityName, parentNode, description }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const [isDocumentationModalVisible, setIsDocumentationModalVisible] = useState(false);

    return (
        <ContentWrapper>
            <CreatedByView actionRequest={actionRequest} />
            <Text color="gray">
                {' '}
                requests to create {entityName} <NameWrapper>{proposedName}</NameWrapper>
            </Text>
            {parentNode && (
                <Text color="gray">
                    {' '}
                    under parent&nbsp;
                    <StyledLink to={`/${entityRegistry.getPathName(EntityType.GlossaryNode)}/${parentNode.urn}`}>
                        <Text type="span">{entityRegistry.getDisplayName(EntityType.GlossaryNode, parentNode)}</Text>
                    </StyledLink>
                </Text>
            )}
            {!parentNode && <Text color="gray"> at the root level</Text>}
            {description && (
                <ViewDocumentationButton variant="text" onClick={() => setIsDocumentationModalVisible(true)}>
                    <FileText size={16} color={colors.gray[500]} />
                    <Text color="gray"> View documentation</Text>
                </ViewDocumentationButton>
            )}
            {isDocumentationModalVisible && (
                <Modal
                    visible
                    footer={null}
                    onCancel={() => setIsDocumentationModalVisible(false)}
                    width={750}
                    title={`${entityName} documentation`}
                >
                    <MDEditor.Markdown style={{ fontWeight: 400 }} source={description} />
                </Modal>
            )}
        </ContentWrapper>
    );
}

export default CreateGlossaryEntityContentView;
