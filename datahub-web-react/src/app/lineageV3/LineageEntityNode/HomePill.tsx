/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Icon, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

const Wrapper = styled.div`
    background-color: ${colors.gray[1500]};
    border-top-left-radius: 4px;
    border-top-right-radius: 4px;
    border: 1px solid ${colors.gray[100]};
    color: ${(p) => p.theme.styles['primary-color']};

    display: flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
    padding: 0 4px;
`;

const StyledIcon = styled(Icon)`
    margin-left: -1px;
`;

interface Props {
    showText: boolean;
}

export default function HomePill({ showText }: Props) {
    return (
        <Wrapper>
            <StyledIcon icon="House" source="phosphor" weight="fill" size="lg" />
            {showText && 'Home'}
        </Wrapper>
    );
}
