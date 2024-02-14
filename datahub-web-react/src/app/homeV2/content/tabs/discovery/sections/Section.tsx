import React from 'react';
import styled from 'styled-components/macro';
import { ANTD_GRAY } from '../../../../../entity/shared/constants';

const Header = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const Title = styled.div`
    color: #403d5c;
    margin: 0px;
    font-size: 18px;
    font-weight: 600;
    margin-bottom: 8px;
`;

const Content = styled.div`
    margin-bottom: 12px;
`;

const Action = styled.div`
    color: ${ANTD_GRAY[8]};
    font-size: 12px;
    font-weight: 700;
    :hover {
        cursor: pointer;
        text-decoration: underline;
    }
    white-space: nowrap;
`;

type Props = {
    title: string;
    children: React.ReactNode;
    actionText?: string;
    onClickAction?: () => void;
};

export const Section = ({ title, actionText, onClickAction, children }: Props) => {
    return (
        <>
            <Header>
                <Title>{title}</Title>
                {actionText && <Action onClick={onClickAction}>{actionText}</Action>}
            </Header>
            <Content>{children}</Content>
        </>
    );
};
