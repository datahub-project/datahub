import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

export const ActionButton = styled.div<{ privilege: boolean }>`
    color: ${(props) => (props.privilege ? `${props.theme.colors.textBrand}` : `${props.theme.colors.textTertiary}`)};
    border: ${(props) =>
        props.privilege
            ? `1px solid ${props.theme.colors.borderBrand}`
            : `1px solid ${props.theme.colors.textTertiary}`};
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
        color: ${props.theme.colors.bg};
        background: ${props.theme.colors.buttonFillBrand};
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
