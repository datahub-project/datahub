import React from 'react';
import { colors, Text } from '@src/alchemy-components';
import { ArrowDown, ArrowElbowDownLeft, ArrowUp } from 'phosphor-react';
import styled from 'styled-components';
import KeyIcon from './KeyIcon';

const Container = styled.div`
    position: sticky;
    bottom: 0;

    border-top: 1px solid ${colors.gray[100]};
    border-radius: 0 0 12px 12px;

    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: flex-end;
    gap: 16px;

    background-color: ${colors.white};
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
    return (
        <Container>
            <KeySuggestion>
                <KeyIcon icon={ArrowUp} />
                <KeyIcon icon={ArrowDown} />
                <Text color="gray" size="sm" weight="semiBold">
                    Navigate
                </Text>
            </KeySuggestion>

            <KeySuggestion>
                <KeyIcon icon={ArrowElbowDownLeft} />
                <Text color="gray" size="sm" weight="semiBold">
                    {isSomethingSelected ? 'Select' : 'Search All'}
                </Text>
            </KeySuggestion>
        </Container>
    );
}
