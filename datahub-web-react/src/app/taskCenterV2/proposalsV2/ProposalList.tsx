import React, { useMemo, useState } from 'react';
import { Button, Checkbox, message, Modal, Pagination, Typography } from 'antd';
import styled from 'styled-components';
import { CheckOutlined, CloseCircleOutlined } from '@ant-design/icons';
import TabToolbar from '@src/app/entityV2/shared/components/styled/TabToolbar';
import analytics, { EntityActionType, EventType } from '@src/app/analytics';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { ActionRequest, ActionRequestAssignee, ActionRequestStatus } from '../../../types.generated';
import { Message } from '../../shared/Message';
import {
    useAcceptProposalsMutation,
    useListActionRequestsQuery,
    useRejectProposalsMutation,
} from '../../../graphql/actionRequest.generated';
import ProposalsTable from './proposalsTable/ProposalsTable';

const ActionRequestsContainer = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    overflow: hidden;
    flex: 1;
    display: flex;
    flex-direction: column;
    ${(props) => props.$isShowNavBarRedesign && 'height: calc(100% - 200px);'}
`;

const Container = styled.div`
    display: contents;
`;

const ActionRequestsTitle = styled(Typography.Title)`
    && {
        margin-bottom: 24px;
    }
`;

const ActionRequestsPaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const CheckboxContainer = styled.div`
    margin-left: 10px;
    display: flex;
    align-items: center;
    gap: 12px;
`;

const BulkActions = styled.div``;

const DEFAULT_PAGE_SIZE = 25;

function containsAll(set, subset) {
    return Array.from(subset).every((elem) => set.has(elem));
}

type Props = {
    title?: string;
    status: ActionRequestStatus;
    assignee?: ActionRequestAssignee;
};

export const ProposalList = ({ title, status, assignee }: Props) => {
    const [page, setPage] = useState(1);
    const [selectedUrns, setSelectedUrns] = useState(new Set<string>());
    const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);
    const [acceptProposalsMutation] = useAcceptProposalsMutation();
    const [rejectProposalsMutation] = useRejectProposalsMutation();
    const isShowNavBarRedesign = useShowNavBarRedesign();

    // Policy list paging.
    const start = (page - 1) * pageSize;

    const { loading, error, data, refetch } = useListActionRequestsQuery({
        variables: {
            input: {
                start,
                count: pageSize,
                status,
                assignee,
            },
        },
        fetchPolicy: 'no-cache',
    });

    let actionRequests = useMemo(() => data?.listActionRequests?.actionRequests || [], [data]);

    // Workaround for lack of read-write lookup consistency.
    if (status === ActionRequestStatus.Pending) {
        // Filter out completed.
        actionRequests = actionRequests.filter((request) => request.status !== ActionRequestStatus.Completed);
    }

    const totalActionRequests = data?.listActionRequests?.total || 0;

    const onChangePage = (newPage: number) => {
        setPage(newPage);
    };

    const onActionRequestUpdate = () => {
        refetch();
    };

    const onSelectPage = (selected: boolean) => {
        // If the urn is already present in selected, unselect, and vice versa.
        const newSelectedUrns = new Set(selectedUrns);
        if (selected) {
            actionRequests?.forEach((request) => newSelectedUrns.add(request.urn));
        } else {
            actionRequests?.forEach((request) => newSelectedUrns.delete(request.urn));
        }
        setSelectedUrns(newSelectedUrns);
    };

    const acceptSelectedProposals = () => {
        Modal.confirm({
            title: 'Accept Proposals',
            content: `Are you sure you want to accept these (${selectedUrns.size}) proposals?`,
            okText: 'Yes',
            onOk() {
                acceptProposalsMutation({ variables: { urns: Array.from(selectedUrns) } })
                    .then(() => {
                        analytics.event({
                            type: EventType.BatchEntityActionEvent,
                            actionType: EntityActionType.ProposalsAccepted,
                            entityUrns: Array.from(selectedUrns),
                        });
                        message.success('Accepted proposals!');
                        refetch();
                        setSelectedUrns(new Set());
                    })
                    .catch((err) => {
                        console.log(err);
                        message.error('Failed to accept proposals. An unexpected error occurred.');
                    });
            },
        });
    };

    const rejectSelectedProposals = () => {
        Modal.confirm({
            title: 'Reject Proposals',
            content: `Are you sure you want to reject these (${selectedUrns.size}) proposals?`,
            okText: 'Yes',
            onOk() {
                rejectProposalsMutation({ variables: { urns: Array.from(selectedUrns) } })
                    .then(() => {
                        analytics.event({
                            type: EventType.BatchEntityActionEvent,
                            actionType: EntityActionType.ProposalsRejected,
                            entityUrns: Array.from(selectedUrns),
                        });
                        message.success('Proposals declined.');
                        refetch();
                        setSelectedUrns(new Set());
                    })
                    .catch((err) => {
                        console.log(err);
                        message.error('Failed to reject proposals. An unexpected error occurred.');
                    });
            },
        });
    };

    const isSelectPage =
        (actionRequests.length &&
            containsAll(
                selectedUrns,
                actionRequests?.map((request) => request.urn),
            )) ||
        false;

    // Somehow need a way to refresh on action request update.
    const selectedCount = selectedUrns.size;

    const FinalContainer = isShowNavBarRedesign ? Container : React.Fragment;

    return (
        <FinalContainer>
            {loading && <Message type="loading" content="Loading your requests…" />}
            {error && message.error('Failed to load proposals. An unknown error occurred!')}
            <TabToolbar>
                <CheckboxContainer>
                    <Checkbox
                        checked={isSelectPage}
                        onChange={(e) => {
                            onSelectPage(e.target.checked as boolean);
                        }}
                    />
                    <Typography.Text strong type="secondary">
                        {selectedCount > 0 ? <>{selectedCount} requests selected</> : null}
                    </Typography.Text>
                </CheckboxContainer>
                <BulkActions>
                    <Button disabled={!selectedUrns.size} onClick={acceptSelectedProposals} type="primary">
                        <CheckOutlined />
                        Approve All
                    </Button>
                    <Button disabled={!selectedUrns.size} onClick={rejectSelectedProposals} type="text">
                        <CloseCircleOutlined />
                        Decline All
                    </Button>
                </BulkActions>
            </TabToolbar>
            <ActionRequestsContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
                {title && <ActionRequestsTitle level={2}>{title}</ActionRequestsTitle>}
                <ProposalsTable
                    actionRequests={actionRequests as ActionRequest[]}
                    isLoading={loading}
                    onActionRequestUpdate={onActionRequestUpdate}
                />

                <ActionRequestsPaginationContainer>
                    <Pagination
                        style={{ margin: 40 }}
                        current={page}
                        pageSize={pageSize}
                        total={totalActionRequests}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger
                        onShowSizeChange={(_currNum, newNum) => setPageSize(newNum)}
                    />
                </ActionRequestsPaginationContainer>
            </ActionRequestsContainer>
        </FinalContainer>
    );
};
