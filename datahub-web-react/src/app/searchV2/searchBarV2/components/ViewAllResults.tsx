import React from 'react';
import { ArrowElbowDownLeft, MagnifyingGlass } from '@phosphor-icons/react';
import { colors, Text } from '@src/alchemy-components';
import styled from 'styled-components/macro';
import KeyIcon from './KeyIcon';

const LeftInternalContainer = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const Container = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
`;

interface Props {
    searchText?: string;
}

export default function ViewAllResults({ searchText }: Props) {
    return (
        <Container>
            <LeftInternalContainer>
                <MagnifyingGlass size={16} color={colors.gray[500]} />
                <Text color="gray" size="lg">
                    View all results for&nbsp;
                    <Text type="span" color="gray" size="lg" weight="semiBold">
                        {searchText}
                    </Text>
                </Text>
            </LeftInternalContainer>
            <KeyIcon icon={ArrowElbowDownLeft} />
        </Container>
    );
}
