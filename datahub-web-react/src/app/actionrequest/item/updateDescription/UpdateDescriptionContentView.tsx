import { blue } from '@ant-design/colors';
import { DiffOutlined } from '@ant-design/icons';
import MDEditor from '@uiw/react-md-editor';
import { Button, Modal, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { ActionRequest, ActionRequestStatus } from '../../../../types.generated';
import CreatedByView from '../CreatedByView';
import RequestTargetEntityView from '../RequestTargetEntityView';
import DescriptionDifferenceModal from './DescriptionDifferenceModal';
import { useGetDescriptionDiffFromActionRequest, useGetEntityNameFromActionRequest } from './utils';

export const ViewDocumentationButton = styled(Button)`
    color: ${blue[5]};
    margin-left: 5px;
    padding: 4px 8px;
`;

interface Props {
    actionRequest: ActionRequest;
}

function UpdateDescriptionContentView({ actionRequest }: Props) {
    const [isDiffModalVisible, setIsDiffModalVisible] = useState(false);
    const [isDescriptionModalVisible, setIsDescriptionModalVisible] = useState(false);

    const entityName = useGetEntityNameFromActionRequest(actionRequest);
    const { oldDescription, newDescription } = useGetDescriptionDiffFromActionRequest(actionRequest);

    const isRequestPending = actionRequest.status === ActionRequestStatus.Pending;

    function handleClick() {
        if (isRequestPending) {
            setIsDiffModalVisible(true);
        } else {
            setIsDescriptionModalVisible(true);
        }
    }

    return (
        <span>
            <CreatedByView actionRequest={actionRequest} />
            <Typography.Text>
                {' '}
                requests to update the description on {entityName}{' '}
                <RequestTargetEntityView actionRequest={actionRequest} />.
                <ViewDocumentationButton type="text" onClick={handleClick}>
                    <DiffOutlined />
                    {isRequestPending ? 'View difference' : 'View description'}
                </ViewDocumentationButton>
            </Typography.Text>
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
        </span>
    );
}

export default UpdateDescriptionContentView;
