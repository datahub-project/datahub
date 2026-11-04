import { Heading } from '@components';
import React from 'react';
import styled from 'styled-components';

const HeaderContainer = styled.div`
    min-height: 32px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 8px;
`;

type Props = {
    title: string;
    actions?: React.ReactNode;
    children?: React.ReactNode;
};

export const SidebarHeader = ({ title, actions, children }: Props) => {
    return (
        <HeaderContainer>
            <Heading type="h5" size="lg" weight="semiBold">
                {title}
            </Heading>
            {actions && <div>{actions}</div>}
            {children}
        </HeaderContainer>
    );
};
