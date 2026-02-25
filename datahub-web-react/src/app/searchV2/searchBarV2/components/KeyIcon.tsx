import { Icon } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';

const IconContainer = styled.div`
    display: flex;
    align-items: center;
    height: 24px;
    width: 32px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 4px;
    padding: 4px 8px;

    & svg {
        color: ${(props) => props.theme.colors.textSecondary};
    }
`;

interface Props {
    icon: Icon;
}

export default function KeyIcon({ icon }: Props) {
    const IconComponent = icon;
    return (
        <IconContainer>
            <IconComponent size={16} />
        </IconContainer>
    );
}
