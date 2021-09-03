import React from 'react';
import styled from 'styled-components';
import { Empty, Typography } from 'antd';
import { EMPTY_MESSAGES } from '../../constants';

const StyledEmpty = styled(Empty)`
    padding: 40px;
    .ant-empty-footer {
        .ant-btn:not(:last-child) {
            margin-right: 8px;
        }
    }
`;

const EmptyDescription = styled.div`
    > * {
        max-width: 70ch;
        display: block;
        margin-right: auto;
        margin-left: auto;
    }
`;

type Props = {
    tab: string;
    children?: React.ReactNode;
};

export const EmptyTab = ({ tab, children }: Props) => {
    return (
        <StyledEmpty
            description={
                <EmptyDescription>
                    <Typography.Title level={4}>{EMPTY_MESSAGES[tab]?.title}</Typography.Title>
                    <Typography.Text type="secondary">{EMPTY_MESSAGES[tab]?.description}</Typography.Text>
                </EmptyDescription>
            }
        >
            {children}
        </StyledEmpty>
    );
};
