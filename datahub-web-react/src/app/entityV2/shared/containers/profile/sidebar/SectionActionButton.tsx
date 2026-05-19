import { Button, Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import type { IconProps } from '@components/components/Icon/types';

export const ActionButton = styled.div<{ privilege: boolean }>`
    color: ${(props) => (props.privilege ? props.theme.colors.textBrand : props.theme.colors.textDisabled)};
    border: 1px solid
        ${(props) => (props.privilege ? props.theme.colors.borderBrand : props.theme.colors.borderDisabled)};
    ${(props) => (props.privilege ? 'cursor: pointer;' : 'pointer-events: none;')}
    height: 20px;
    width: 20px;
    border-radius: 50%;
    text-align: center;
    svg {
        width: 18px;
        height: 18px;
        padding: 3px;
    }
    :hover {
        ${(props) =>
            props.privilege &&
            `
        color: ${props.theme.colors.textOnFillBrand};
        background: ${props.theme.colors.buttonFillBrand};
        `}
    
`;

type Props = {
    tip?: string;
    icon?: IconProps['icon'];
    button?: React.ReactNode;
    onClick: any;
    actionPrivilege?: boolean;
    dataTestId?: string;
};

const SectionActionButton = ({ tip, icon, button, onClick, actionPrivilege = true, dataTestId }: Props) => {
    const tooltipTitle = tip ?? (!actionPrivilege ? 'You do not have permission to change this.' : undefined);
    // Wrapper needed so the Tooltip has a hover target: the disabled inner control gets
    // `pointer-events: none`, which swallows mouse events and prevents the Tooltip from
    // firing when disabled.
    const wrapperStyle: React.CSSProperties = {
        display: 'inline-block',
        cursor: !actionPrivilege ? 'not-allowed' : undefined,
    };
    // When disabled, stop click propagation at the wrapper so a click on the action doesn't
    // bubble up and trigger the parent SidebarSection's collapse/expand handler.
    const wrapperOnClick = !actionPrivilege ? (e: React.MouseEvent) => e.stopPropagation() : undefined;
    return (
        <Tooltip placement="top" title={tooltipTitle} showArrow={false}>
            {icon ? (
                // eslint-disable-next-line jsx-a11y/click-events-have-key-events,jsx-a11y/no-static-element-interactions
                <span style={wrapperStyle} onClick={wrapperOnClick}>
                    <Button
                        variant="text"
                        color="violet"
                        size="md"
                        icon={{ icon }}
                        onClick={onClick}
                        disabled={!actionPrivilege}
                        data-testid={dataTestId}
                    />
                </span>
            ) : (
                // eslint-disable-next-line jsx-a11y/click-events-have-key-events,jsx-a11y/no-static-element-interactions
                <span style={wrapperStyle} onClick={wrapperOnClick}>
                    <ActionButton onClick={onClick} privilege={actionPrivilege} data-testid={dataTestId}>
                        {button}
                    </ActionButton>
                </span>
            )}
        </Tooltip>
    );
};

export default SectionActionButton;
