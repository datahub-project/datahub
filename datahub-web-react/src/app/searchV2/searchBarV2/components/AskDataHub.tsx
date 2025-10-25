import { ArrowElbowDownLeft, Sparkle } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components/macro';

import KeyIcon from '@app/searchV2/searchBarV2/components/KeyIcon';
import { Text, colors } from '@src/alchemy-components';

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

export default function AskDataHub({ searchText }: Props) {
    return (
        <Container>
            <LeftInternalContainer>
                <Sparkle size={16} color={colors.violet[500]} weight="fill" />
                <Text color="gray">
                    <Text type="span" color="violet" weight="semiBold">
                        Ask DataHub
                    </Text>
                    &nbsp;about&nbsp;
                    <Text type="span" color="gray" weight="semiBold">
                        {searchText}
                    </Text>
                </Text>
            </LeftInternalContainer>
            <KeyIcon icon={ArrowElbowDownLeft} />
        </Container>
    );
}
