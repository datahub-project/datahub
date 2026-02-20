import { MagnifyingGlass } from '@phosphor-icons/react';
import React from 'react';
import { useTheme } from 'styled-components';
import styled from 'styled-components/macro';

import { Text } from '@src/alchemy-components';

const RecommendedOptionWrapper = styled.div`
    margin-left: 0;
    display: flex;
    align-items: center;
    gap: 8px;
`;

interface Props {
    text: string;
    dataTestId?: string;
}

export default function RecentSearch({ text, dataTestId }: Props) {
    const theme = useTheme();
    return (
        <RecommendedOptionWrapper data-testid={dataTestId}>
            <MagnifyingGlass size={20} color={theme.colors.textSecondary} />
            <Text weight="semiBold">{text}</Text>
        </RecommendedOptionWrapper>
    );
}
