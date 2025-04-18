import { Modal, Typography } from 'antd';
import React, { useState } from 'react';
import { FileTextOutlined } from '@ant-design/icons';
import MDEditor from '@uiw/react-md-editor';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { ActionRequest, EntityType, GlossaryNode } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import CreatedByView from './CreatedByView';
import { ViewDocumentationButton } from './updateDescription/UpdateDescriptionContentView';

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
    const entityRegistry = useEntityRegistry();
    const [isDocumentationModalVisible, setIsDocumentationModalVisible] = useState(false);

    return (
        <span>
            <CreatedByView actionRequest={actionRequest} />
            <Typography.Text>
                {' '}
                requests to create {entityName} <NameWrapper>{proposedName}</NameWrapper>
                {parentNode && (
                    <Typography.Text>
                        {' '}
                        under parent&nbsp;
                        <Link to={`/${entityRegistry.getPathName(EntityType.GlossaryNode)}/${parentNode.urn}`}>
                            <Typography.Text strong>
                                {entityRegistry.getDisplayName(EntityType.GlossaryNode, parentNode)}
                            </Typography.Text>
                        </Link>
                    </Typography.Text>
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
            </Typography.Text>
        </span>
    );
}

export default CreateGlossaryEntityContentView;
