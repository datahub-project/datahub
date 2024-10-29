import React, { useState } from 'react';
import styled from 'styled-components';
import { List, Pagination } from 'antd';
import { ANTD_GRAY_V2 } from '@src/app/entity/shared/constants';
import { useListSubscriptionsQuery } from '../../../graphql/subscriptions.generated';
import { scrollToTop } from '../../shared/searchUtils';
import { useEntityRegistry } from '../../useEntityRegistry';
import { PreviewType } from '../Entity';
import { CompactUserSubscriptions } from './CompactUserSubscriptions';
import { SidebarSection } from '../shared/containers/profile/sidebar/SidebarSection';

const UserSubscriptionsWrapper = styled.div`
    height: calc(100vh - 114px);
    overflow: auto;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const StyledPagination = styled(Pagination)`
    margin: 40px;
`;

export const StyledList = styled(List)`
    overflow-y: auto;
    height: 100%;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    background-color: ${ANTD_GRAY_V2[1]};
    padding: 12px;
    flex: 1;
    &::-webkit-scrollbar {
        height: 12px;
        width: 5px;
        background: #f2f2f2;
    }
    &::-webkit-scrollbar-thumb {
        background: #cccccc;
        -webkit-border-radius: 1ex;
        -webkit-box-shadow: 0px 1px 2px rgba(0, 0, 0, 0.75);
    }
` as typeof List;

export const ListItem = styled.div<{ isSelectMode: boolean }>`
    padding: 20px;
    display: flex;
    align-items: center;
    background-color: #ffffff;
    border-radius: 10px;
    overflow: hidden;
    transition: margin-bottom 0.3s ease;
    border: 1px solid #ebecf0;
`;

const PAGE_SIZE = 10;

type Props = {
    isCompact?: boolean;
    urn?: string;
};

export const UserSubscriptions = ({ isCompact, urn }: Props) => {
    const [page, setPage] = useState(1);
    const entityRegistry = useEntityRegistry();
    const start = (page - 1) * PAGE_SIZE;
    const { data: listSubscriptionData } = useListSubscriptionsQuery({
        variables: { input: { start, count: PAGE_SIZE, groupUrn: undefined, actorUrn: urn } },
    });

    const subscriptions = listSubscriptionData?.listSubscriptions?.subscriptions || [];
    const totalSubscriptions = listSubscriptionData?.listSubscriptions?.total || 0;

    const onChangePage = (newPage: number) => {
        scrollToTop();
        setPage(newPage);
    };

    return isCompact ? (
        <SidebarSection
            title="Subscribed to"
            content={<CompactUserSubscriptions subscriptions={subscriptions} />}
            count={subscriptions?.length}
        />
    ) : (
        <UserSubscriptionsWrapper>
            <StyledList
                dataSource={subscriptions}
                renderItem={(subscription) => {
                    const { entity } = subscription;
                    return (
                        <ListItem isSelectMode={false}>
                            {entityRegistry.renderPreview(entity.type, PreviewType.SEARCH, entity)}
                        </ListItem>
                    );
                }}
            />
            {totalSubscriptions >= PAGE_SIZE && (
                <PaginationContainer>
                    <StyledPagination
                        current={page}
                        pageSize={PAGE_SIZE}
                        total={totalSubscriptions}
                        showLessItems
                        onChange={onChangePage}
                        showSizeChanger={false}
                    />
                </PaginationContainer>
            )}
        </UserSubscriptionsWrapper>
    );
};
