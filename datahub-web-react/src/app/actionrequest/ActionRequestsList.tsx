import React, { useMemo, useState } from 'react';
import { Empty, List, message, Pagination, Typography } from 'antd';
import styled from 'styled-components';
import ActionRequestListItem from './item/ActionRequestListItem';
import { ActionRequest, ActionRequestAssignee, ActionRequestStatus } from '../../types.generated';
import { Message } from '../shared/Message';
import { useListActionRequestsQuery } from '../../graphql/actionRequest.generated';

const ActionRequestsContainer = styled.div``;

const ActionRequestsTitle = styled(Typography.Title)`
    && {
        margin-bottom: 24px;
    }
`;

const ActionRequestsStyledList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        box-shadow: ${(props) => props.theme.styles['box-shadow']};
    }
`;

const ActionRequestsPaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const DEFAULT_PAGE_SIZE = 25;

type Props = {
    title?: string;
    status: ActionRequestStatus;
    assignee?: ActionRequestAssignee;
};

export const ActionRequestsList = ({ title, status, assignee }: Props) => {
    const [page, setPage] = useState(1);

    // Policy list paging.
    const pageSize = DEFAULT_PAGE_SIZE;
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

    // Somehow need a way to refresh on action request update.

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading your requests..." />}
            {error && message.error('Failed to load your requests :(')}
            <ActionRequestsContainer>
                {title && <ActionRequestsTitle level={2}>{title}</ActionRequestsTitle>}
                <ActionRequestsStyledList
                    bordered
                    locale={{
                        emptyText: <Empty description="No Requests!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                    }}
                    dataSource={actionRequests}
                    renderItem={(item: unknown) => (
                        <ActionRequestListItem
                            actionRequest={item as ActionRequest}
                            onUpdate={onActionRequestUpdate}
                            showActionsButtons
                        />
                    )}
                />
                <ActionRequestsPaginationContainer>
                    <Pagination
                        style={{ margin: 40 }}
                        current={page}
                        pageSize={pageSize}
                        total={totalActionRequests}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </ActionRequestsPaginationContainer>
            </ActionRequestsContainer>
        </>
    );
};
