import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

const SecondaryText = styled(Text)`
    color: ${(props) => props.theme.colors.textSecondary};
`;

interface Props {
    text?: string;
}

export default function ModuleDescription({ text }: Props) {
    if (!text) return null;
    return (
        <SecondaryText size="md" weight="medium" lineHeight="xs">
            {text}
        </SecondaryText>
    );
}
