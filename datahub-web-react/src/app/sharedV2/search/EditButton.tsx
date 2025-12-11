/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Tooltip, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

const StyledButton = styled(Button)`
    border: 1px solid ${colors.gray[100]};
`;

type Props = {
    setShowSelectMode: (showSelectMode: boolean) => any;
    disabled?: boolean;
};

export default function EditButton({ setShowSelectMode, disabled }: Props) {
    return (
        <Tooltip title="Edit..." showArrow={false} placement="top">
            <StyledButton
                onClick={() => setShowSelectMode(true)}
                disabled={disabled}
                data-testid="search-results-edit-button"
                isCircle
                icon={{ icon: 'PencilSimple', source: 'phosphor' }}
                variant="text"
                color="gray"
                size="sm"
            />
        </Tooltip>
    );
}
