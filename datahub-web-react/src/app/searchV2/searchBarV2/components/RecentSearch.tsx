import React from 'react';
import { MagnifyingGlass } from '@phosphor-icons/react';
import { Text } from '@src/alchemy-components';
import colors from '@src/alchemy-components/theme/foundations/colors';
import styled from 'styled-components/macro';

const RecommendedOptionWrapper = styled.div`
    margin-left: 0;
    display: flex;
    align-items: center;
    gap: 8px;
`;

interface Props {
    text: string;
}

export default function RecentSearch({ text }: Props) {
    return (
        <RecommendedOptionWrapper>
            <MagnifyingGlass size={20} color={colors.gray[500]} />
            <Text weight="semiBold">{text}</Text>
        </RecommendedOptionWrapper>
    );
}
