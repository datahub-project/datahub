import React, { useMemo, useState } from 'react';
import { message, Typography } from 'antd';
import styled from 'styled-components';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { Pagination } from '@src/alchemy-components';
import { ActionRequest, ActionRequestAssignee, ActionRequestStatus } from '../../../types.generated';
import { Message } from '../../shared/Message';
import { useListActionRequestsQuery } from '../../../graphql/actionRequest.generated';
import ProposalsTable from './proposalsTable/ProposalsTable';
import ActionsBar from './ActionsBar';

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

const FooterContainer = styled.div`
    margin-top: 15px;
`;

const ActionRequestsTitle = styled(Typography.Title)`
    && {
        margin-bottom: 24px;
    }
`;

const DEFAULT_PAGE_SIZE = 25;

type Props = {
    title?: string;
    status: ActionRequestStatus;
    assignee?: ActionRequestAssignee;
};

export const ProposalList = ({ title, status, assignee }: Props) => {
    const [page, setPage] = useState(1);
    const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const [selectedUrns, setSelectedUrns] = useState<string[]>([]);

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

    const FinalContainer = isShowNavBarRedesign ? Container : React.Fragment;

    return (
        <FinalContainer>
            {loading && <Message type="loading" content="Loading your requests…" />}
            {error && message.error('Failed to load proposals. An unknown error occurred!')}

            <ActionRequestsContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
                {title && <ActionRequestsTitle level={2}>{title}</ActionRequestsTitle>}
                <ProposalsTable
                    actionRequests={actionRequests as ActionRequest[]}
                    isLoading={loading}
                    onActionRequestUpdate={onActionRequestUpdate}
                    selectedKeys={selectedUrns}
                    setSelectedKeys={setSelectedUrns}
                />
                <FooterContainer>
                    {selectedUrns.length > 0 && (
                        <ActionsBar
                            selectedUrns={selectedUrns}
                            setSelectedUrns={setSelectedUrns}
                            onActionRequestUpdate={onActionRequestUpdate}
                        />
                    )}
                    <Pagination
                        currentPage={page}
                        itemsPerPage={pageSize}
                        totalPages={totalActionRequests}
                        onPageChange={onChangePage}
                        showSizeChanger
                        onShowSizeChange={(_currNum, newNum) => setPageSize(newNum)}
                        loading={loading}
                        hideOnSinglePage
                        showLessItems
                    />
                </FooterContainer>
            </ActionRequestsContainer>
        </FinalContainer>
    );
};
