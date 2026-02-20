import { ArrowDown, ArrowElbowDownLeft, ArrowUp } from 'phosphor-react';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import KeyIcon from '@app/searchV2/searchBarV2/components/KeyIcon';
import { Text } from '@src/alchemy-components';

const Container = styled.div`
    position: sticky;
    bottom: 0;

    border-top: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 0 0 12px 12px;

    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: flex-end;
    gap: 16px;

    background-color: ${(props) => props.theme.colors.bg};
    height: 36px;
    width: 100%;

    padding: 8px 16px;
`;

const KeySuggestion = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 8px;
`;

interface Props {
    isSomethingSelected?: boolean;
}

export default function AutocompleteFooter({ isSomethingSelected }: Props) {
    const theme = useTheme();
    return (
        <Container>
            <KeySuggestion>
                <KeyIcon icon={ArrowUp} />
                <KeyIcon icon={ArrowDown} />
                <Text size="sm" weight="semiBold" style={{ color: theme.colors.textSecondary }}>
                    Navigate
                </Text>
            </KeySuggestion>

            <KeySuggestion>
                <KeyIcon icon={ArrowElbowDownLeft} />
                <Text size="sm" weight="semiBold" style={{ color: theme.colors.textSecondary }}>
                    {isSomethingSelected ? 'Select' : 'Search All'}
                </Text>
            </KeySuggestion>
        </Container>
    );
}
