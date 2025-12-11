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

const DepthMarginContainer = styled.div<{ $depth: number }>`
    margin-left: calc(16px * ${(props) => props.$depth});
`;

interface Props {
    depth: number;
}

export default function DepthMargin({ depth }: Props) {
    return <DepthMarginContainer $depth={depth} />;
}
