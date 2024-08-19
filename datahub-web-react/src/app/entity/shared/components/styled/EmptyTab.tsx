import React from 'react';
import styled from 'styled-components';
import { Empty, Typography } from 'antd';
import { translateDisplayNames } from '../../../../../utils/translation/translation';
import { useTranslation } from 'react-i18next';

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
    const { t } = useTranslation();

    return (
        <StyledEmpty
            description={
                <EmptyDescription>
                    <Typography.Title level={4}>{translateDisplayNames(t, 'empty' + tab + 'title')}</Typography.Title>
                    <Typography.Text type="secondary">{translateDisplayNames(t, 'empty' + tab + 'description')}</Typography.Text>
                </EmptyDescription>
            }
        >
            {children}
        </StyledEmpty>
    );
};
