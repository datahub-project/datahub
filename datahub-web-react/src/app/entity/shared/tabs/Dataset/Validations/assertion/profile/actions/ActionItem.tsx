import React from 'react';

import { Tooltip } from 'antd';

import { ActionItemButton } from './styledComponents';

type Props = {
    primary?: boolean;
    tip?: string;
    disabled?: boolean;
    onClick: () => void;
    icon: React.ReactNode;
    key?: string;
};

export const ActionItem = ({ primary = false, tip, disabled = false, onClick, icon, key }: Props) => {
    return (
        <Tooltip placement="left" title={tip}>
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
        </Tooltip>
    );
};
