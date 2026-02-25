import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

const TitleContainer = styled.span`
    display: flex;
    align-items: center;
    justify-content: left;
    padding-left: 0px;
`;

const IconContainer = styled.span`
    && {
        color: ${(props) => props.theme.colors.textSecondary};
        margin-right: 12px;
    }
`;

type Props = {
    tip?: React.ReactNode;
    title: string;
    icon: React.ReactNode;
};

/**
 * Base Item Title for the menu
 */
export const IconItemTitle = ({ tip, title, icon }: Props) => {
    return (
        <Tooltip title={tip} placement="right">
            <TitleContainer>
                <IconContainer>{icon}</IconContainer>
                {title}
            </TitleContainer>
        </Tooltip>
    );
};
