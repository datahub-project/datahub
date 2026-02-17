import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components/macro';


const Header = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const Title = styled.div`
    color: ${(props) => props.theme.colors.text};
    margin: 0px;
    font-size: 18px;
    font-weight: 600;
    margin-bottom: 8px;
`;

const Content = styled.div`
    margin-bottom: 20px;
    position: relative;
    &:hover {
        .hover-btn {
            display: flex;
        }
    }
`;

const Action = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
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
    tip?: string;
    children: React.ReactNode;
    actionText?: string;
    onClickAction?: () => void;
};

export const Section = ({ title, tip, actionText, onClickAction, children }: Props) => {
    return (
        <>
            <Header>
                <Tooltip title={tip}>
                    <Title>{title}</Title>
                </Tooltip>
                {actionText && <Action onClick={onClickAction}>{actionText}</Action>}
            </Header>
            <Content>{children}</Content>
        </>
    );
};
