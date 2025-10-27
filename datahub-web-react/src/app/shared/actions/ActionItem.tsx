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
    return (
        <Tooltip placement={placement} title={isExpandedView ? '' : tip}>
            <StyledActionButtonContainer
                onClick={(e) => {
                    e.stopPropagation();
                    if (disabled) return;
                    onClick();
                    onActionTriggered?.();
                }}
            >
                <ActionMenuItem disabled={disabled} title={!isExpandedView ? tip : undefined} data-testid={dataTestId}>
                    {icon}
                </ActionMenuItem>
                {isExpandedView && actionName && <span>{actionName}</span>}
            </StyledActionButtonContainer>
        </Tooltip>
    );
};
