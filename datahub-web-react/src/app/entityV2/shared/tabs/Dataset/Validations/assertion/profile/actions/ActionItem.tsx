import React from 'react';

import { Tooltip } from 'antd';

import { ActionItemButton } from './styledComponents';
import { TooltipPlacement } from 'antd/es/tooltip';

type Props = {
    primary?: boolean;
    tip?: string;
    disabled?: boolean;
    onClick: () => void;
    icon: React.ReactNode;
    key?: string;
    placement?: TooltipPlacement;
};

export const ActionItem = ({
    primary = false,
    tip,
    disabled = false,
    onClick,
    icon,
    key,
    placement = 'top',
}: Props) => {
    return (
        <Tooltip placement={placement} title={tip}>
            <span style={{ cursor: disabled ? 'not-allowed' : 'pointer' }}>
                <ActionItemButton
                    primary={primary}
                    key={key}
                    disabled={disabled}
                    onClick={(e) => {
                        e.stopPropagation();
                        if (disabled) return;
                        onClick();
                    }}
                >
                    {icon}
                </ActionItemButton>
            </span>
        </Tooltip>
    );
};
