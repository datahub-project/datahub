import React from 'react';
import styled from 'styled-components';
import { Tooltip } from '@components';
import { REDESIGN_COLORS } from '../../../constants';

export const ActionButton = styled.div<{ privilege: boolean }>`
    color: ${(props) =>
        props.privilege ? `${REDESIGN_COLORS.TITLE_PURPLE}` : `${REDESIGN_COLORS.SECONDARY_LIGHT_GREY}`};
    border: ${(props) =>
        props.privilege
            ? `1px solid ${REDESIGN_COLORS.TITLE_PURPLE}`
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
        background: ${REDESIGN_COLORS.TITLE_PURPLE};
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
        <Tooltip placement="top" title={!actionPrivilege ? 'No access' : tip} showArrow={false}>
            <>
                <ActionButton onClick={onClick} privilege={actionPrivilege} data-testid={dataTestId}>
                    {button}
                </ActionButton>
            </>
        </Tooltip>
    );
};

export default SectionActionButton;
