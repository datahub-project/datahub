import { Tooltip } from '@components';
import { capitalCase } from '@remirror/core';
import { useHelpers, useI18n } from '@remirror/react';
import { Button, ButtonProps } from 'antd';
import React, { MouseEventHandler, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const StyledButton = styled(Button)`
    height: auto;
`;

export interface CommandButtonProps extends Omit<ButtonProps, 'type'> {
    active?: boolean;
    children?: React.ReactNode;
    commandName?: string;
}

export const CommandButton = ({ active, children, commandName, ...buttonProps }: CommandButtonProps) => {
    const { getCommandOptions } = useHelpers();
    const { t } = useI18n();
    const { t: translate } = useTranslation('alchemy');
    const options = commandName ? getCommandOptions(commandName) : undefined;

    // Resolve the tooltip in three tiers: Remirror's own localized label when the
    // command ships one, then our `alchemy` translation for commands Remirror doesn't
    // label, then the capitalCased command name as a final fallback.
    const tooltipTitle = (() => {
        if (!commandName) return null;
        const remirrorLabel =
            typeof options?.label === 'function'
                ? options.label({ enabled: true, active: !!active, attrs: undefined, t })
                : options?.label;
        if (remirrorLabel) return remirrorLabel;
        return translate(`editor.command.${commandName}`, {
            defaultValue: capitalCase(options?.name ?? commandName),
        });
    })();

    const handleMouseDown: MouseEventHandler<HTMLButtonElement> = useCallback((e) => {
        e.preventDefault();
    }, []);

    return (
        <Tooltip title={tooltipTitle}>
            <StyledButton
                type={active ? 'link' : 'text'}
                onMouseDown={handleMouseDown}
                {...buttonProps}
                data-testid={`command-${commandName}-btn`}
            >
                {children}
            </StyledButton>
        </Tooltip>
    );
};
