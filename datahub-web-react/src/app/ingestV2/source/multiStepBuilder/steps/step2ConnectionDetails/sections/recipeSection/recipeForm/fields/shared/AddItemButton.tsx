import { Button, Icon, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

export const CentredButton = styled(Button)`
    justify-content: center;
`;

interface Props {
    onClick: () => void;
    text?: string;
}

export function AddItemButton({ onClick, text }: Props) {
    return (
        <CentredButton
            onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                onClick();
            }}
            variant="text"
            type="button"
            size="xs"
        >
            <Icon source="phosphor" icon="Plus" size="lg" />
            <Text>{text}</Text>
        </CentredButton>
    );
}
