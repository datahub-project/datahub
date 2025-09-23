import { Icon, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    justify-content: center;
    align-items: center;
    padding: 8px;
`;

interface Props {
    message?: string;
}

export default function HiddenItemsMessage({ message }: Props) {
    return (
        <Container>
            <Icon source="phosphor" icon="Lock" size="lg" />{' '}
            <Text weight="bold" size="sm">
                {message}
            </Text>{' '}
            <Text size="sm">Contact your admin for access</Text>
        </Container>
    );
}
