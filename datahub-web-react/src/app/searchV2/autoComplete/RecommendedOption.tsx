import { Icon } from '@components';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import React from 'react';
import styled from 'styled-components/macro';

import { SuggestionText } from '@app/searchV2/autoComplete/styledComponents';

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
            <Icon icon={MagnifyingGlass} />
            <TextWrapper>{text}</TextWrapper>
        </RecommendedOptionWrapper>
    );
}
