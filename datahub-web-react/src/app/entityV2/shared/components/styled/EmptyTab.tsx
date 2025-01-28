import React from 'react';
import styled from 'styled-components';
import { Empty, Typography } from 'antd';
import { EMPTY_MESSAGES } from '../../constants';
import NoDocs from '../../../../../images/no-docs.svg';

const StyledEmpty = styled(Empty)<{ $hideImage?: boolean }>`
    padding: 40px;
    ${({ $hideImage }) => ($hideImage ? '.ant-empty-image { margin: 0; }' : '.ant-empty-image { height: 86px; }')}

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
    hideImage?: boolean;
    children?: React.ReactNode;
};

export const EmptyTab = ({ tab, hideImage, children }: Props) => {
    return (
        <StyledEmpty
            description={
                <EmptyDescription data-testid="empty-tab-description">
                    <Typography.Title level={4}>{EMPTY_MESSAGES[tab]?.title}</Typography.Title>
                    <Typography.Text type="secondary">{EMPTY_MESSAGES[tab]?.description}</Typography.Text>
                </EmptyDescription>
            }
            $hideImage={hideImage}
            image={NoDocs}
        >
            {children}
        </StyledEmpty>
    );
};
