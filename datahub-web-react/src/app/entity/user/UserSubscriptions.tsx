import React, { useState } from 'react';
import styled from 'styled-components';
import { Pagination } from 'antd';
import { useListSubscriptionsQuery } from '../../../graphql/subscriptions.generated';
import { scrollToTop } from '../../shared/searchUtils';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ListItem, StyledList, ThinDivider } from '../../recommendations/renderer/component/EntityNameList';
import { PreviewType } from '../Entity';

const UserSubscriptionsWrapper = styled.div`
    height: calc(100vh - 114px);
    overflow: auto;
    padding-left: 10px;
`;

const PaginationContainer = styled.div`
    display: flex;
    justify-content: center;
`;

const StyledPagination = styled(Pagination)`
    margin: 40px;
`;

const PAGE_SIZE = 10;

type Props = {
    urn: string;
};

export const UserSubscriptions = ({ urn }: Props) => {
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

    return (
        <UserSubscriptionsWrapper>
            <StyledList
                dataSource={subscriptions}
                renderItem={(subscription) => {
                    const { entity } = subscription;
                    return (
                        <>
                            <ListItem isSelectMode={false}>
                                {entityRegistry.renderPreview(entity.type, PreviewType.PREVIEW, entity)}
                            </ListItem>
                            <ThinDivider />
                        </>
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
