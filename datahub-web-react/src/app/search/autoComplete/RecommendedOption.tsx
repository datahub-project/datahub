import { Icon } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import { SuggestionText } from '@app/search/autoComplete/styledComponents';

const TextWrapper = styled.span``;

const RecommendedOptionWrapper = styled(SuggestionText)`
    margin-left: 0;
    display: flex;
    align-items: center;

    ${TextWrapper} {
        margin-left: 8px;
    }
`;

interface Props {
    text: string;
}

export default function RecommendedOption({ text }: Props) {
    return (
        <RecommendedOptionWrapper>
            <Icon icon="MagnifyingGlass" source="phosphor" />
            <TextWrapper>{text}</TextWrapper>
        </RecommendedOptionWrapper>
    );
}
