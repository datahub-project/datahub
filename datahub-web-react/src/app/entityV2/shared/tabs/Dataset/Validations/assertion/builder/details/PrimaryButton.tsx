/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button, Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

const Icon = styled.div`
    margin-right: 8px;
`;

type Props = {
    icon?: React.ReactNode;
    title: string;
    disabled?: boolean;
    tooltip?: React.ReactNode;
    onClick: () => void;
    dataTestId?: string;
};

export const PrimaryButton = ({ icon, title, tooltip, disabled = false, onClick, dataTestId }: Props) => {
    return (
        <Tooltip title={tooltip} placement="left" showArrow={false}>
            <Button
                disabled={disabled}
                onClick={(e) => {
                    e.stopPropagation();
                    onClick();
                }}
                data-testid={dataTestId}
            >
                {(icon && <Icon>{icon}</Icon>) || null}
                {title}
            </Button>
        </Tooltip>
    );
};
