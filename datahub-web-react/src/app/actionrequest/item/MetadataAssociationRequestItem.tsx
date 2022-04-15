import { CheckOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { Button, message, Modal, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { useAcceptProposalMutation, useRejectProposalMutation } from '../../../graphql/actionRequest.generated';
import { ActionRequest, ActionRequestResult, ActionRequestStatus, EntityType } from '../../../types.generated';
import { CustomAvatar } from '../../shared/avatar';
import { capitalizeFirstLetter } from '../../shared/textUtil';
import { useEntityRegistry } from '../../useEntityRegistry';

type Props = {
    actionRequest: ActionRequest;
    requestTypeDisplayName: string;
    requestMetadataView: React.ReactNode;
    onUpdate: () => void;
    showActionsButtons: boolean;
};

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
`;

const RequestTypeContainer = styled.div`
    width: 200px;
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

const RequestContentContainer = styled.span``;

/**
 * Base list item for showing metadata association proposals.
 */
export default function MetadataAssociationRequestItem({
    actionRequest,
    requestTypeDisplayName,
    requestMetadataView,
    onUpdate,
    showActionsButtons,
}: Props) {
    const entityRegistry = useEntityRegistry();

    const [acceptProposalMutation] = useAcceptProposalMutation();
    const [rejectProposalMutation] = useRejectProposalMutation();

    const acceptRequest = () => {
        Modal.confirm({
            content: 'Are you sure you want to accept this proposal?',
            okText: 'Yes',
            onOk() {
                acceptProposalMutation({ variables: { urn: actionRequest.urn } })
                    .then(() => {
                        message.success('Successfully accepted the proposal!');
                        onUpdate();
                    })
                    .catch((err) => {
                        console.log(err);
                        message.error('Failed to accept proposal. :(');
                    });
            },
        });
    };

    const rejectRequest = () => {
        Modal.confirm({
            content: 'Are you sure you want to reject this proposal?',
            okText: 'Yes',
            onOk() {
                rejectProposalMutation({ variables: { urn: actionRequest.urn } })
                    .then(() => {
                        message.info('Proposal declined.');
                        onUpdate();
                    })
                    .catch((err) => {
                        console.log(err);
                        message.error('Failed to reject proposal. :(');
                    });
            },
        });
    };

    /**
     * Build the date column view.
     */
    const createdDate = new Date(actionRequest.created.time).toLocaleDateString('en-US'); // Todo format this correctly.
    const createdDateView = <Typography.Text>{createdDate}</Typography.Text>;

    /**
     * Build the request type view.
     */
    let suffix = '';
    if (actionRequest.result === ActionRequestResult.Accepted) {
        suffix = ' Accepted';
    }
    if (actionRequest.result === ActionRequestResult.Rejected) {
        suffix = ' Rejected';
    }

    const requestTypeView = (
        <RequestTypeContainer>
            <Typography.Text strong>
                {requestTypeDisplayName}
                {suffix}
            </Typography.Text>
        </RequestTypeContainer>
    );

    /**
     * Build the request content column view.
     */
    // 1. Build the author view.
    const createdBy = actionRequest.created.actor;
    const createdByDisplayName =
        (createdBy && entityRegistry.getDisplayName(EntityType.CorpUser, createdBy)) || 'Anonymous';
    const createdByDisplayImage = createdBy && createdBy.editableInfo?.pictureLink;
    const createdByProfileUrl = `/${entityRegistry.getPathName(EntityType.CorpUser)}/${createdBy?.urn}`;
    const createdByView = (
        <AuthorView>
            <CustomAvatar
                name={createdByDisplayName}
                url={createdByProfileUrl}
                photoUrl={createdByDisplayImage || undefined}
            />
            <Link to={createdByProfileUrl}>
                <AuthorText strong>{createdByDisplayName}</AuthorText>
            </Link>
        </AuthorView>
    );

    // 2. Build the target entity view.
    const requestTargetEntityType = actionRequest.entity?.type;
    const requestTargetDisplayName =
        requestTargetEntityType && entityRegistry.getDisplayName(requestTargetEntityType, actionRequest.entity);
    const requestTargetEntityView = requestTargetEntityType && (
        <>
            <Link to={`/${entityRegistry.getPathName(requestTargetEntityType)}/${actionRequest.entity?.urn}`}>
                <Typography.Text strong>{requestTargetDisplayName}</Typography.Text>
            </Link>
            {!!actionRequest.subResource && (
                <>
                    {' '}
                    field <Typography.Text strong>{actionRequest.subResource}</Typography.Text>
                </>
            )}
        </>
    );

    // 3. Combine author, metadata, target entity view to create full request content view.
    const requestContentView = (
        <RequestContentContainer>
            {createdByView}
            <Typography.Text> requests to add </Typography.Text>
            {requestMetadataView}
            {showActionsButtons && <Typography.Text>{` to `}</Typography.Text>}
            {showActionsButtons && requestTargetEntityView}
        </RequestContentContainer>
    );

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
                <Button onClick={acceptRequest}>
                    <CheckOutlined />
                    Approve & Add
                </Button>
                <Button type="text" onClick={rejectRequest}>
                    <CloseCircleOutlined />
                    Decline
                </Button>
            </>
        );
    }

    return (
        <ContentContainer>
            <LeftContentContainer>
                <LeftContentContainerItem>{createdDateView}</LeftContentContainerItem>
                <LeftContentContainerItem>{requestTypeView}</LeftContentContainerItem>
                <LeftContentContainerItem>{requestContentView}</LeftContentContainerItem>
            </LeftContentContainer>
            {showActionsButtons && <RightContentContainer>{actionResultView}</RightContentContainer>}
        </ContentContainer>
    );
}
