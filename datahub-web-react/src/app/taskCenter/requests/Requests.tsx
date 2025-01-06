import React, { useMemo } from 'react';

import { Empty, List, message } from 'antd';
import styled from 'styled-components';
import { FormForActor } from '@src/types.generated';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { Message } from '../../shared/Message';
import { useGetFormsForActorQuery } from '../../../graphql/form.generated';

import { RequestItem } from './RequestItem';
import { filterFormsForUser } from './utils';

const StyledList = styled(List)<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        overflow-x: hidden;
        overflow-y: auto;
        height: calc(100% - 125px);
        border-radius: 0 0 ${props.theme.styles['border-radius-navbar-redesign']} ${props.theme.styles['border-radius-navbar-redesign']};
    `}

    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }
`;

export const Requests = () => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const { data, loading, error, refetch } = useGetFormsForActorQuery({
        variables: { input: { searchFlags: { skipCache: true } } },
        fetchPolicy: 'no-cache',
    });

    const requests = useMemo(
        () =>
            // client-side filter of forms unfinished
            (data?.getFormsForActor.formsForActor || []).filter((form) => filterFormsForUser(form as FormForActor)) ||
            [],
        [data],
    );

    return (
        <>
            {!data && loading && <Message type="loading" content="Loading your requests…" />}
            {error && message.error('Failed to load compliance requests. An unknown error occurred!')}
            <StyledList
                bordered
                locale={{
                    emptyText: <Empty description="No Requests!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                }}
                dataSource={requests}
                renderItem={(item: unknown) => <RequestItem request={item} refetch={refetch} />}
                $isShowNavBarRedesign={isShowNavBarRedesign}
            />
        </>
    );
};
