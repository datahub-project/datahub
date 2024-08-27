import React from 'react';
import { Typography } from 'antd';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import { translateDisplayNames } from '../../../../../../utils/translation/translation';

const HeaderContainer = styled.div`
    min-height: 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 8px;
    > .ant-typography {
        margin-bottom: 0;
    }
`;

type Props = {
    title: string;
    actions?: React.ReactNode;
    children?: React.ReactNode;
};

export const SidebarHeader = ({ title, actions, children }: Props) => {
    const { t } = useTranslation();

    return (
        <HeaderContainer>
            <Typography.Title level={5}>{translateDisplayNames(t, title)}</Typography.Title>
            {actions && <div>{actions}</div>}
            {children}
        </HeaderContainer>
    );
};
