import { FileTextOutlined } from '@ant-design/icons';
import { Text } from '@components';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { ActionRequest, EntityType, GlossaryNode } from '@src/types.generated';
import MDEditor from '@uiw/react-md-editor';
import { Modal, Typography } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import CreatedByView from './CreatedByView';
import { ViewDocumentationButton } from './updateDescription/UpdateDescriptionRequestItem';
import { ContentWrapper } from './styledComponents';

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
            <Text color="gray" type="span">
                {' '}
                requests to create {entityName} <NameWrapper>{proposedName}</NameWrapper>
                {parentNode && (
                    <Text>
                        {' '}
                        under parent&nbsp;
                        <Link to={`/${entityRegistry.getPathName(EntityType.GlossaryNode)}/${parentNode.urn}`}>
                            <Text weight="bold">
                                {entityRegistry.getDisplayName(EntityType.GlossaryNode, parentNode)}
                            </Text>
                        </Link>
                    </Text>
                )}
                {!parentNode && <Typography.Text> at the root level</Typography.Text>}
                {description && (
                    <ViewDocumentationButton type="text" onClick={() => setIsDocumentationModalVisible(true)}>
                        <FileTextOutlined /> View documentation
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
            </Text>
        </ContentWrapper>
    );
}

export default CreateGlossaryEntityContentView;
