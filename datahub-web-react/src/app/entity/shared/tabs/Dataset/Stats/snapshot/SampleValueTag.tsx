/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tag, Tooltip } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

const StyledTag = styled(Tag)`
    cursor: pointer;
    max-width: 250px;
    overflow: hidden;
    text-overflow: ellipsis;

    &&:hover {
        color: ${(props) => props.theme.styles['primary-color']};
        border-color: ${(props) => props.theme.styles['primary-color']};
    }
`;

type Props = {
    value: string;
};

export default function SampleValueTag({ value }: Props) {
    const [copied, setCopied] = useState(false);

    const onClick = () => {
        setCopied(true);
        navigator.clipboard.writeText(value);
        setTimeout(() => setCopied(false), 2000);
    };

    return (
        <Tooltip title={copied ? 'Copied' : 'Click to copy'}>
            <StyledTag onClick={onClick}>{value}</StyledTag>
        </Tooltip>
    );
}
