import { CheckOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { Button, Modal, Typography, message } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { CustomAvatar } from '@app/shared/avatar';
import { capitalizeFirstLetter } from '@app/shared/textUtil';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useAcceptProposalsMutation, useRejectProposalsMutation } from '@graphql/actionRequest.generated';
import { ActionRequest, ActionRequestResult, ActionRequestStatus, EntityType } from '@types';

type Props = {
    actionRequest: ActionRequest;
    requestTypeDisplayName: string;
    requestContentView: React.ReactNode;
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
    margin-left: 10px;
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

const DateText = styled(Typography.Text)`
    white-space: nowrap;
`;

const RightContentContainer = styled.div`
    display: flex;
    justify-content: right;
    align-items: middle;
`;

const ResultStatusView = styled.div`
    white-space: nowrap;
`;

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
            content: 'Are you sure you want to accept this proposal?',
            okText: 'Yes',
            onOk() {
                acceptProposalsMutation({ variables: { urns: [actionRequest.urn] } })
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
                rejectProposalsMutation({ variables: { urns: [actionRequest.urn] } })
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
    const createdDateView = <DateText>{createdDate}</DateText>;

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
            <ResultStatusView>
                {resultStatusView}
                <Typography.Text> by </Typography.Text>
                {resultAuthorView}
            </ResultStatusView>
        );
    } else {
        actionResultView = (
            <>
                <Button onClick={acceptRequest}>
                    <CheckOutlined />
                    Approve
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
