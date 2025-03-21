import React from 'react';
import styled from 'styled-components';
import { Tooltip, colors } from '@components';
import { REDESIGN_COLORS } from '../../../constants';

export const ActionButton = styled.div<{ privilege: boolean }>`
    color: ${(props) => (props.privilege ? `${colors.gray[600]};` : `${REDESIGN_COLORS.SECONDARY_LIGHT_GREY}`)};
    ${(props) => (props.privilege ? 'cursor: pointer;' : 'pointer-events: none;')}
    height: 24px;
    width: 24px;
    border-radius: 50%;
    text-align: center;
    display: flex;
    align-items: center;
    justify-content: center;
    
    :hover {
        ${(props) =>
            props.privilege &&
            `
        color: ${colors.gray[900]};
        background: ${colors.gray[100]};
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
