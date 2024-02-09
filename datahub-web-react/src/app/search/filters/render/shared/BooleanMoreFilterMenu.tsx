import React from 'react';
import { Button } from 'antd';
import styled from 'styled-components/macro';

const StyledButton = styled(Button)`
    width: 100%;
    text-align: center;
    background-color: ${(props) => props.theme.styles['primary-color']};
    color: white;
    border-radius: 0;
`;

export const DropdownMenu = styled.div<{ alignRight?: boolean }>`
    background-color: white;
    border-radius: 5px;
    box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 6px 16px 0 rgba(0, 0, 0, 0.08), 0 9px 28px 8px rgba(0, 0, 0, 0.05);
    overflow: hidden;
    min-width: 200px;

    ${(props) =>
        props.alignRight &&
        `
    position: absolute;
    left: 205px;
    top: -34px;
    `}

    .ant-dropdown-menu-title-content {
        background-color: white;
        &:hover {
            background-color: white;
        }
    }
`;

const ScrollableContent = styled.div`
    max-height: 312px;
    overflow: auto;
`;

interface Props {
    menuOption: React.ReactNode;
    onUpdate: () => void;
    alignRight?: boolean;
}

export default function BooleanMoreFilterMenu({ menuOption, onUpdate, alignRight }: Props) {
    return (
        <DropdownMenu alignRight={alignRight} data-testid="boolean-filter-dropdown">
            <ScrollableContent>
                {React.cloneElement(menuOption as React.ReactElement, { style: { boxShadow: 'none' } })}
            </ScrollableContent>
            <StyledButton type="text" onClick={onUpdate} data-testid="boolean-update-filters">
                Update
            </StyledButton>
        </DropdownMenu>
    );
}
