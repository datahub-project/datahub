import { Button, Modal, Text, colors } from '@components';
import MDEditor from '@uiw/react-md-editor';
import DOMPurify from 'dompurify';
import { GitDiff } from 'phosphor-react';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import CreatedByView from '@app/actionrequestV2/item/CreatedByView';
import RequestTargetEntityView from '@app/actionrequestV2/item/RequestTargetEntityView';
import { ContentWrapper } from '@app/actionrequestV2/item/styledComponents';
import DescriptionDifferenceModal from '@app/actionrequestV2/item/updateDescription/DescriptionDifferenceModal';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { ActionRequest, ActionRequestStatus } from '@types';

export const ViewDocumentationButton = styled(Button)`
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
            <ViewDocumentationButton
                variant="text"
                onClick={(e) => {
                    handleClick();
                    e.stopPropagation();
                }}
            >
                <GitDiff size={16} color={colors.gray[500]} />
                <Text color="gray">{isRequestPending ? 'View difference' : 'View description'}</Text>
            </ViewDocumentationButton>
            <div
                onClick={(e) => e.stopPropagation()}
                role="button"
                tabIndex={0}
                onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                        e.stopPropagation();
                    }
                }}
            >
                {isDiffModalVisible && (
                    <DescriptionDifferenceModal
                        oldDescription={oldDescription}
                        newDescription={newDescription}
                        closeModal={() => setIsDiffModalVisible(false)}
                        actionRequest={actionRequest}
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
                        <MDEditor.Markdown
                            style={{ fontWeight: 400, color: colors.gray[500] }}
                            source={newDescription}
                        />
                    </Modal>
                )}
            </div>
        </ContentWrapper>
    );
}

export default UpdateDescriptionRequestItem;
