import { Button, Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import type { IconNames } from '@components/components/Icon/types';

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
    icon?: IconNames;
    button?: React.ReactNode;
    onClick: any;
    actionPrivilege?: boolean;
    dataTestId?: string;
};

const SectionActionButton = ({ tip, icon, button, onClick, actionPrivilege = true, dataTestId }: Props) => {
    return (
        <Tooltip
            placement="top"
            title={!actionPrivilege ? 'You do not have permission to change this.' : tip}
            showArrow={false}
        >
            {icon ? (
                <span>
                    <Button
                        variant="text"
                        color="violet"
                        size="md"
                        icon={{ icon, source: 'phosphor' }}
                        onClick={onClick}
                        disabled={!actionPrivilege}
                        data-testid={dataTestId}
                    />
                </span>
            ) : (
                <ActionButton onClick={onClick} privilege={actionPrivilege} data-testid={dataTestId}>
                    {button}
                </ActionButton>
            )}
        </Tooltip>
    );
};

export default SectionActionButton;
