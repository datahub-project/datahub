import { colors, Icon, Pill } from '@src/alchemy-components';
import analytics, { EventType, EntityActionType } from '@src/app/analytics';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useAcceptProposalsMutation, useRejectProposalsMutation } from '@src/graphql/actionRequest.generated';
import { ActionRequest, ActionRequestResult, ActionRequestStatus, EntityType } from '@src/types.generated';
import { message, Modal } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

const IconsContainer = styled.div`
    display: flex;
    justify-content: end;
    gap: 8px;
`;

const StyledIcon = styled(Icon)`
    border-radius: 50%;
    padding: 3px;

    :hover {
        cursor: pointer;
    }
`;

interface Props {
    actionRequest: ActionRequest;
    onUpdate: () => void;
    showPendingView?: boolean;
}

const ActionsColumn = ({ actionRequest, onUpdate, showPendingView }: Props) => {
    const entityRegistry = useEntityRegistryV2();

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

    let actionResultView;

    if (actionRequest.status === ActionRequestStatus.Completed) {
        const resultAuthor = actionRequest.lastModified?.actor; // Who completed the request.
        const isAccepted = actionRequest.result === ActionRequestResult.Accepted;
        const resultAuthorDisplayName =
            resultAuthor && entityRegistry.getDisplayName(EntityType.CorpUser, resultAuthor);
        actionResultView = (
            <div>
                <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${resultAuthor?.urn}`}>
                    <Pill
                        leftIcon={isAccepted ? 'Check' : 'Close'}
                        label={resultAuthorDisplayName || ''}
                        color={isAccepted ? 'green' : 'red'}
                        size="md"
                    />
                </Link>
            </div>
        );
    } else if (showPendingView) {
        // Just showing the status until we support deleting a proposal
        actionResultView = (
            <Pill
                size="md"
                customIconRenderer={() => <Icon size="md" icon="Spinner" source="phosphor" />}
                label="Pending"
            />
        );
    } else {
        actionResultView = (
            <IconsContainer>
                <StyledIcon
                    icon="Close"
                    color="red"
                    style={{ backgroundColor: colors.red[0] }}
                    onClick={(e) => {
                        e.stopPropagation();
                        rejectRequest();
                    }}
                    data-testid="decline-button"
                />
                <StyledIcon
                    icon="Check"
                    color="green"
                    style={{ backgroundColor: colors.green[0] }}
                    onClick={(e) => {
                        e.stopPropagation();
                        acceptRequest();
                    }}
                    data-testid="approve-button"
                />
            </IconsContainer>
        );
    }

    return <>{actionResultView}</>;
};

export default ActionsColumn;
