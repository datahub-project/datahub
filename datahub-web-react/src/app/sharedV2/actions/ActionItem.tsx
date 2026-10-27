import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { ActionMenuItem } from '@src/app/entityV2/shared/EntityDropdown/styledComponents';

type TooltipPlacement = NonNullable<React.ComponentProps<typeof Tooltip>['placement']>;

const StyledActionButtonContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

type Props = {
    tip?: React.ReactNode;
    disabled?: boolean;
    onClick: () => void;
    icon: React.ReactNode;
    placement?: TooltipPlacement;
    isExpandedView?: boolean;
    actionName?: string;
    dataTestId?: string;
    onActionTriggered?: () => void;
};

export const ActionItem = ({
    tip,
    disabled = false,
    onClick,
    icon,
    placement = 'top',
    isExpandedView = false,
    actionName,
    dataTestId,
    onActionTriggered,
}: Props) => {
    const tooltipTitle = isExpandedView && !disabled ? '' : tip;
    const actionTitle = typeof tip === 'string' && !isExpandedView ? tip : undefined;

    return (
        <Tooltip placement={placement} title={tooltipTitle}>
            <StyledActionButtonContainer
                onClick={(e) => {
                    e.stopPropagation();
                    if (disabled) return;
                    onClick();
                    onActionTriggered?.();
                }}
            >
                <ActionMenuItem disabled={disabled} title={actionTitle} data-testid={dataTestId}>
                    {icon}
                </ActionMenuItem>
                {isExpandedView && actionName && <span>{actionName}</span>}
            </StyledActionButtonContainer>
        </Tooltip>
    );
};
