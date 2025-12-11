/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { capitalCase } from '@remirror/core';
import { useHelpers } from '@remirror/react';
import { Button, ButtonProps, Tooltip } from 'antd';
import React, { MouseEventHandler, useCallback } from 'react';

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
