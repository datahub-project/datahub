import React from 'react';
import { Tooltip } from '@components';
import { TooltipPlacement } from 'antd/es/tooltip';
import styled from 'styled-components';
import { ActionItemButton } from './styledComponents';

const StyledActionButtonContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    color: #46507b;
`;

type Props = {
    primary?: boolean;
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
    primary = false,
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
                style={{ cursor: disabled ? 'not-allowed' : 'pointer' }}
                onClick={(e) => {
                    e.stopPropagation();
                    if (disabled) return;
                    onClick();
                }}
            >
                <ActionItemButton
                    primary={primary}
                    key={key}
                    disabled={disabled}
                    title={!isExpandedView ? tip : undefined}
                    isExpandedView={isExpandedView}
                    data-testid={dataTestId}
                >
                    {icon}
                </ActionItemButton>
                {isExpandedView && actionName && <span>{actionName}</span>}
            </StyledActionButtonContainer>
        </Tooltip>
    );
};
