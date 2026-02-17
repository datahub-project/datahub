import React from 'react';
import styled from 'styled-components';


type Props = {
    icon: any;
    onClick?: () => void;
    enabled?: boolean;
};

const IconContainer = styled.div<{ enabled?: boolean }>`
    width: 28px;
    height: 28px;
    background-color: ${(props) => props.theme.colors.bgSurface};
    cursor: pointer;
    border-radius: 50%;
    text-align: center;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    & svg {
        font-size: 14px;
        color: ${(props) => props.theme.colors.textTertiary};
    }
    border: 1px solid ${(props) => props.theme.colors.border};
    :hover {
        border: 1px solid ${({ enabled, theme }) => (enabled ? theme.styles['primary-color'] : theme.colors.bgSurface)};
    }
`;

const CardActionCircle = ({ icon, onClick, enabled }: Props) => {
    return (
        <IconContainer enabled={enabled} onClick={onClick}>
            {icon}
        </IconContainer>
    );
};

export default CardActionCircle;
