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

const StyledValue = styled.div`
    font-family: Manrope;
    font-size: 14px;
    font-style: normal;
    font-weight: 400;
    line-height: 22px;
    color: #373d44;
`;

const StyledDescription = styled.div`
    font-family: Manrope;
    font-size: 12px;
    font-style: normal;
    font-weight: 500;
    line-height: 16px;
    color: #5e666e;
`;

interface Props {
    value: string | number | null;
    description?: string | null;
}

export default function DropdownLabel({ value, description }: Props) {
    return (
        <>
            <StyledValue>{value}</StyledValue>
            <StyledDescription>{description}</StyledDescription>
        </>
    );
}
