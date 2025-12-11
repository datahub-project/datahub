/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from '@components';
import { TooltipPlacement } from 'antd/es/tooltip';
import React from 'react';
import styled from 'styled-components';

import { ActionMenuItem } from '@src/app/entityV2/shared/EntityDropdown/styledComponents';

const StyledActionButtonContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

type Props = {
    tip?: string;
    disabled?: boolean;
    onClick: () => void;
    icon: React.ReactNode;
    key?: string;
    placement?: TooltipPlacement;
    isExpandedView?: boolean;
    actionName?: string;
    dataTestId?: string;
};

export const ActionItem = ({
    tip,
    disabled = false,
    onClick,
    icon,
    key,
    placement = 'top',
    isExpandedView = false,
    actionName,
    dataTestId,
}: Props) => {
    return (
        <Tooltip placement={placement} title={isExpandedView ? '' : tip}>
            <StyledActionButtonContainer
                onClick={(e) => {
                    e.stopPropagation();
                    if (disabled) return;
                    onClick();
                }}
            >
                <ActionMenuItem
                    key={key}
                    disabled={disabled}
                    title={!isExpandedView ? tip : undefined}
                    data-testid={dataTestId}
                >
                    {icon}
                </ActionMenuItem>
                {isExpandedView && actionName && <span>{actionName}</span>}
            </StyledActionButtonContainer>
        </Tooltip>
    );
};
