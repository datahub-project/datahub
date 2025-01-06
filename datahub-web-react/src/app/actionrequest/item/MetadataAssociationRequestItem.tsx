import { CheckOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { Button, message, Modal, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { ANTD_GRAY } from '@src/app/entity/shared/constants';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { useAcceptProposalsMutation, useRejectProposalsMutation } from '../../../graphql/actionRequest.generated';
import { ActionRequest, ActionRequestStatus, EntityType } from '../../../types.generated';
import { CustomAvatar } from '../../shared/avatar';
import { capitalizeFirstLetter } from '../../shared/textUtil';
import { useEntityRegistry } from '../../useEntityRegistry';
import analytics, { EntityActionType, EventType } from '../../analytics';

const ContentContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
    min-height: 32px;
`;

const LeftContentContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

const LeftContentContainerItem = styled.div`
    margin-left: 20px;
    margin-right: 20px;
    font-size: 14px;
`;

const RequestTypeContainer = styled.div`
    width: 200px;
    color: ${ANTD_GRAY[8]};
`;

const AuthorView = styled.span`
    margin-left: 4px;
`;

const AuthorText = styled(Typography.Text)`
    margin-left: 2px;
`;

const RightContentContainer = styled.div`
    display: flex;
    justify-content: right;
    align-items: middle;
`;

type Props = {
    actionRequest: ActionRequest;
    requestTypeDisplayName: string;
    requestContentView: React.ReactNode;
    onUpdate: () => void;
    showActionsButtons: boolean;
};

/**
 * Base list item for showing metadata association proposals.
 */
export default function MetadataAssociationRequestItem({
    actionRequest,
    requestTypeDisplayName,
    requestContentView,
    onUpdate,
    showActionsButtons,
}: Props) {
    const entityRegistry = useEntityRegistry();

    const [acceptProposalsMutation] = useAcceptProposalsMutation();
    const [rejectProposalsMutation] = useRejectProposalsMutation();

    const acceptRequest = () => {
        Modal.confirm({
            title: 'Accept Proposal',
            content: 'Are you sure you want to accept this proposal?',
            okText: 'Yes',
            onOk() {
                acceptProposalsMutation({ variables: { urns: [actionRequest.urn] } })
                    .then(() => {
                        if (actionRequest.entity?.urn) {
                            analytics.event({
                                type: EventType.EntityActionEvent,
                                actionType: EntityActionType.ProposalAccepted,
                                actionQualifier: actionRequest.type,
                                entityType: actionRequest.entity?.type,
                                entityUrn: actionRequest.entity?.urn,
                            });
                        }
                        message.success('Successfully accepted the proposal!');
                        onUpdate();
                    })
                    .catch((err) => {
                        console.log(err);
                        message.error('Failed to accept proposal. An unknown error occurred!');
                    });
            },
        });
    };

    const rejectRequest = () => {
        Modal.confirm({
            title: 'Reject Proposal',
            content: 'Are you sure you want to reject this proposal?',
            okText: 'Yes',
            onOk() {
                rejectProposalsMutation({ variables: { urns: [actionRequest.urn] } })
                    .then(() => {
                        if (actionRequest.entity?.urn) {
                            analytics.event({
                                type: EventType.EntityActionEvent,
                                actionType: EntityActionType.ProposalRejected,
                                actionQualifier: actionRequest.type,
                                entityType: actionRequest.entity?.type,
                                entityUrn: actionRequest.entity?.urn,
                            });
                        }
                        message.info('Proposal declined.');
                        onUpdate();
                    })
                    .catch((err) => {
                        console.log(err);
                        message.error('Failed to reject proposal. An unknown error occurred!');
                    });
            },
        });
    };

    /**
     * Build the date column view.
     */
    const createdDate = new Date(actionRequest.created.time).toLocaleDateString('en-US'); // Todo format this correctly.
    const createdDateView = <Typography.Text>{createdDate}</Typography.Text>;

    const requestTypeView = <RequestTypeContainer>{requestTypeDisplayName}</RequestTypeContainer>;

    const createdBy = actionRequest.created.actor;
    const createdByDisplayImage = createdBy && createdBy.editableInfo?.pictureLink;

    /**
     * Create the request action / result view. (right side)
     */

    let actionResultView;

    if (actionRequest.status === ActionRequestStatus.Completed) {
        const resultAuthor = actionRequest.lastModified?.actor; // Who completed the request.
        const result = (actionRequest.result && capitalizeFirstLetter(actionRequest.result)) || 'Completed';
        const resultStatusView = <Typography.Text strong>{result}</Typography.Text>;
        const resultAuthorDisplayName =
            resultAuthor && entityRegistry.getDisplayName(EntityType.CorpUser, resultAuthor);
        const resultAuthorView = resultAuthor && (
            <AuthorView>
                <CustomAvatar
                    name={entityRegistry.getDisplayName(EntityType.CorpUser, resultAuthor)}
                    url={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${resultAuthor?.urn}`}
                    photoUrl={createdByDisplayImage || undefined}
                />
                <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${resultAuthor?.urn}`}>
                    <AuthorText strong>{resultAuthorDisplayName}</AuthorText>
                </Link>
            </AuthorView>
        );
        actionResultView = (
            <div>
                {resultStatusView}
                <Typography.Text> by </Typography.Text>
                {resultAuthorView}
            </div>
        );
    } else {
        actionResultView = (
            <>
                <Button
                    type="primary"
                    style={{ background: REDESIGN_COLORS.TITLE_PURPLE }}
                    onClick={acceptRequest}
                    data-testid="approve-button"
                >
                    <CheckOutlined />
                    Approve
                </Button>
                <Button type="text" onClick={rejectRequest} data-testid="decline-button">
                    <CloseCircleOutlined />
                    Decline
                </Button>
            </>
        );
    }

    return (
        <ContentContainer>
            <LeftContentContainer>
                {showActionsButtons && <LeftContentContainerItem>{createdDateView}</LeftContentContainerItem>}
                {showActionsButtons && <LeftContentContainerItem>{requestTypeView}</LeftContentContainerItem>}
                <LeftContentContainerItem>{requestContentView}</LeftContentContainerItem>
            </LeftContentContainer>
            {showActionsButtons && <RightContentContainer>{actionResultView}</RightContentContainer>}
        </ContentContainer>
    );
}
