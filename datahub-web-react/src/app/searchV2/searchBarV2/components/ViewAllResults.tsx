/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ArrowElbowDownLeft, MagnifyingGlass } from '@phosphor-icons/react';
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
    dataTestId?: string;
}

export default function ViewAllResults({ searchText, dataTestId }: Props) {
    return (
        <Container data-testid={dataTestId}>
            <LeftInternalContainer>
                <MagnifyingGlass size={16} color={colors.gray[500]} />
                <Text color="gray">
                    View all results for&nbsp;
                    <Text type="span" color="gray" weight="semiBold">
                        {searchText}
                    </Text>
                </Text>
            </LeftInternalContainer>
            <KeyIcon icon={ArrowElbowDownLeft} />
        </Container>
    );
}
