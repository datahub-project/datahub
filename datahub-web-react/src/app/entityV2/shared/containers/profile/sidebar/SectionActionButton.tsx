import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

export const ActionButton = styled.div<{ privilege: boolean }>`
    color: ${(props) =>
        props.privilege ? `${props.theme.styles['primary-color']}` : `${REDESIGN_COLORS.SECONDARY_LIGHT_GREY}`};
    border: ${(props) =>
        props.privilege
            ? `1px solid ${props.theme.styles['primary-color']}`
            : `1px solid ${REDESIGN_COLORS.SECONDARY_LIGHT_GREY}`};
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
        color: ${REDESIGN_COLORS.WHITE};
        background: ${props.theme.styles['primary-color']};
        `}
    
`;

type Props = {
    tip?: string;
    button: React.ReactNode;
    onClick: any;
    actionPrivilege?: boolean;
    dataTestId?: string;
};

const SectionActionButton = ({ tip, button, onClick, actionPrivilege = true, dataTestId }: Props) => {
    return (
        <Tooltip
            placement="top"
            title={!actionPrivilege ? 'You do not have permission to change this.' : tip}
            showArrow={false}
        >
            <>
                <ActionButton onClick={onClick} privilege={actionPrivilege} data-testid={dataTestId}>
                    {button}
                </ActionButton>
            </>
        </Tooltip>
    );
};

export default SectionActionButton;
