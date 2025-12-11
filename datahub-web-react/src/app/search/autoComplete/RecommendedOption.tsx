/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
