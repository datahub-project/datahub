import { ArrowElbowDownLeft, MagnifyingGlass } from '@phosphor-icons/react';
import React from 'react';
import { useTheme } from 'styled-components';
import styled from 'styled-components/macro';

import KeyIcon from '@app/searchV2/searchBarV2/components/KeyIcon';
import { Text } from '@src/alchemy-components';

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
    dataTestId?: string;
}

export default function ViewAllResults({ searchText, dataTestId }: Props) {
    const theme = useTheme();
    return (
        <Container data-testid={dataTestId}>
            <LeftInternalContainer>
                <MagnifyingGlass size={16} color={theme.colors.textSecondary} />
                <Text style={{ color: theme.colors.textSecondary }}>
                    View all results for&nbsp;
                    <Text type="span" weight="semiBold" style={{ color: theme.colors.textSecondary }}>
                        {searchText}
                    </Text>
                </Text>
            </LeftInternalContainer>
            <KeyIcon icon={ArrowElbowDownLeft} />
        </Container>
    );
}
