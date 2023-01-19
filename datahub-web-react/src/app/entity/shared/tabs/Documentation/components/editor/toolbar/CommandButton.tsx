import React, { MouseEventHandler, useCallback } from 'react';
import { Button, ButtonProps, Tooltip } from 'antd';
import { capitalCase } from '@remirror/core';
import { useHelpers } from '@remirror/react';

export interface CommandButtonProps extends Omit<ButtonProps, 'type'> {
    active?: boolean;
    children?: React.ReactNode;
    commandName?: string;
}

export const CommandButton = ({ active, children, commandName, ...buttonProps }: CommandButtonProps) => {
    const { getCommandOptions } = useHelpers();
    const options = commandName ? getCommandOptions(commandName) : undefined;

    const handleMouseDown: MouseEventHandler<HTMLButtonElement> = useCallback((e) => {
        e.preventDefault();
    }, []);

    return (
        <Tooltip title={options?.name ? capitalCase(options?.name) : null}>
            <Button type={active ? 'link' : 'text'} onMouseDown={handleMouseDown} {...buttonProps}>
                {children}
            </Button>
        </Tooltip>
    );
};
