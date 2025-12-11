/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { OverflowText, Text } from '@components';
import React from 'react';
import styled from 'styled-components';

const TextWithMaxWidth = styled(Text)<{ $maxWidth?: string; $disableWrapping?: boolean }>`
    ${(props) => props.$maxWidth && `max-width: ${props.$maxWidth};`}
    ${(props) => props.$disableWrapping && `text-wrap: nowrap;`}
`;

interface Props {
    text: string;
    maxWidth?: string;
    disableWrapping?: boolean;
}

export default function TextValue({ text, maxWidth, disableWrapping }: Props) {
    return (
        <TextWithMaxWidth color="gray" $maxWidth={maxWidth} $disableWrapping={disableWrapping}>
            <OverflowText text={text} />
        </TextWithMaxWidth>
    );
}
