/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { MagnifyingGlass } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components/macro';

import { Text } from '@src/alchemy-components';
import colors from '@src/alchemy-components/theme/foundations/colors';

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
    return (
        <RecommendedOptionWrapper data-testid={dataTestId}>
            <MagnifyingGlass size={20} color={colors.gray[500]} />
            <Text weight="semiBold">{text}</Text>
        </RecommendedOptionWrapper>
    );
}
