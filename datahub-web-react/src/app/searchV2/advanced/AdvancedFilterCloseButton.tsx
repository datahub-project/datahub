/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CloseOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';

const CloseSpan = styled.span`
    :hover {
        color: black;
        cursor: pointer;
    }
`;

interface Props {
    onClose: () => void;
}

export default function AdvancedFilterCloseButton({ onClose }: Props) {
    return (
        <CloseSpan
            role="button"
            onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                onClose();
            }}
            tabIndex={0}
            onKeyPress={onClose}
        >
            <CloseOutlined />
        </CloseSpan>
    );
}
