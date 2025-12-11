/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { Text, spacing } from '@src/alchemy-components';

const StyledPopoverContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: ${spacing.xxsm};
`;

const StyledPopoverRowContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: ${spacing.sm};
    justify-content: space-between;
`;

type GraphPopoverProps = {
    header: React.ReactNode;
    value: React.ReactNode;
    pills: React.ReactNode;
};

export default function GraphPopover({ header, value, pills }: GraphPopoverProps) {
    return (
        <StyledPopoverContainer>
            <Text color="gray" size="sm" type="div">
                {header}
            </Text>
            <StyledPopoverRowContainer>
                <Text color="gray" size="sm" weight="bold">
                    {value}
                </Text>
                {pills}
            </StyledPopoverRowContainer>
        </StyledPopoverContainer>
    );
}
