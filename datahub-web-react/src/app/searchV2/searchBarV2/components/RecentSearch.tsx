import { MagnifyingGlass } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components/macro';

import { Text } from '@src/alchemy-components';

const RecommendedOptionWrapper = styled.div`
    margin-left: 0;
    display: flex;
    align-items: center;
    gap: 8px;
    color: ${(props) => props.theme.colors.text};
`;

const SearchIcon = styled.span`
    display: inline-flex;
    color: ${(props) => props.theme.colors.icon};
`;

interface Props {
    text: string;
    dataTestId?: string;
}

export default function RecentSearch({ text, dataTestId }: Props) {
    return (
        <RecommendedOptionWrapper data-testid={dataTestId}>
            <SearchIcon>
                <MagnifyingGlass size={20} />
            </SearchIcon>
            <Text>{text}</Text>
        </RecommendedOptionWrapper>
    );
}
