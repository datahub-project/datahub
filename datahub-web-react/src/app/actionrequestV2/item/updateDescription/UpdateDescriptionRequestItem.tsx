import { blue } from '@ant-design/colors';
import { DiffOutlined } from '@ant-design/icons';
import { Text } from '@components';
import MDEditor from '@uiw/react-md-editor';
import { Button, Modal } from 'antd';
import DOMPurify from 'dompurify';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { ActionRequest, ActionRequestStatus } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import CreatedByView from '../CreatedByView';
import RequestTargetEntityView from '../RequestTargetEntityView';
import DescriptionDifferenceModal from './DescriptionDifferenceModal';
import { ContentWrapper } from '../styledComponents';

export const ViewDocumentationButton = styled(Button)`
    color: ${blue[5]};
    margin-left: 5px;
    padding: 4px 8px;
`;

interface Props {
    actionRequest: ActionRequest;
}

function UpdateDescriptionRequestItem({ actionRequest }: Props) {
    const [isDiffModalVisible, setIsDiffModalVisible] = useState(false);
    const [isDescriptionModalVisible, setIsDescriptionModalVisible] = useState(false);
    const entityRegistry = useEntityRegistry();

    let entityName = '';
    if (actionRequest.entity) {
        entityName = entityRegistry.getEntityName(actionRequest.entity.type) || '';
    }
    const newDescription = DOMPurify.sanitize(actionRequest.params?.updateDescriptionProposal?.description || '');
    const oldDescription = DOMPurify.sanitize(
        (actionRequest.entity as any)?.editableProperties?.description ||
            (actionRequest.entity as any)?.properties?.description ||
            '',
    );
    const isRequestPending = actionRequest.status === ActionRequestStatus.Pending;

    function handleClick() {
        if (isRequestPending) {
            setIsDiffModalVisible(true);
        } else {
            setIsDescriptionModalVisible(true);
        }
    }

    return (
        <ContentWrapper>
            <CreatedByView actionRequest={actionRequest} />
            <Text color="gray" weight="medium">
                {' '}
                requests to update the description on {entityName}{' '}
            </Text>
            <RequestTargetEntityView actionRequest={actionRequest} />
            <ViewDocumentationButton type="text" onClick={handleClick}>
                <DiffOutlined />
                {isRequestPending ? 'View difference' : 'View description'}
            </ViewDocumentationButton>
            {isDiffModalVisible && (
                <DescriptionDifferenceModal
                    oldDescription={oldDescription}
                    newDescription={newDescription}
                    closeModal={() => setIsDiffModalVisible(false)}
                />
            )}
            {isDescriptionModalVisible && (
                <Modal
                    visible
                    footer={null}
                    onCancel={() => setIsDescriptionModalVisible(false)}
                    width={750}
                    title="Update Description Proposal"
                >
                    <MDEditor.Markdown style={{ fontWeight: 400 }} source={newDescription} />
                </Modal>
            )}
        </ContentWrapper>
    );
}

export default UpdateDescriptionRequestItem;
