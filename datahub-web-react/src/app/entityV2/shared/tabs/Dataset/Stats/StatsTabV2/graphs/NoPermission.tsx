/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { EmptyMessageContainer } from '@src/alchemy-components/components/GraphCard/components';

const EmptyMessageWrapper = styled.div`
    text-align: center;
`;

interface Props {
    statName: string;
}

const NoPermission = ({ statName }: Props) => {
    return (
        <EmptyMessageContainer data-testid="no-permissions">
            <EmptyMessageWrapper>
                <Text size="2xl" weight="bold" color="gray">
                    No Permission
                </Text>
                <Text color="gray">{`You do not have permission to view ${statName} data.`}</Text>
            </EmptyMessageWrapper>
        </EmptyMessageContainer>
    );
};

export default NoPermission;
