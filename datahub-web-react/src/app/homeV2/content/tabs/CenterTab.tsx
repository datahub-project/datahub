import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import { CountBadge } from '@app/homeV2/content/tabs/CountBadge';

const Tab = styled.div<{ selected: boolean; disabled: boolean }>`
    font-size: 14px;
    line-height: 22px;
    padding: 10px 16px;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    ${(props) => !props.selected && `color: ${props.theme.colors.text};`}
    ${(props) => props.disabled && `color: ${props.theme.colors.textDisabled};`}
    ${(props) => props.selected && `background-color: ${props.theme.colors.buttonFillBrand};`}
    ${(props) => props.selected && `color: ${props.theme.colors.textOnFillBrand};`}
    ${(props) =>
        !props.disabled &&
        `:hover {
            cursor: pointer;
            ${!props.selected && `color: ${props.theme.colors.textHover};`}
        }`}
`;

const Name = styled.div`
    font-size: 14px;
`;

const tabIconStyle = {
    fontSize: '16px',
    marginRight: '10px',
};

type Props = {
    id?: string;
    name: string;
    description?: string;
    icon?: any;
    onClick: () => void;
    selected: boolean;
    count?: number;
    disabled?: boolean;
};

export const CenterTab = ({ id, name, description, icon: Icon, selected, count, disabled = false, onClick }: Props) => {
    return (
        <Tooltip title={description} placement="bottom" showArrow={false}>
            <Tab
                id={id}
                key={name}
                onClick={() => (!disabled ? onClick() : () => null)}
                selected={selected}
                disabled={disabled}
            >
                {Icon && <Icon style={tabIconStyle} />}
                <Name>{name}</Name>
                {(count && <CountBadge count={count} />) || null}
            </Tab>
        </Tooltip>
    );
};
