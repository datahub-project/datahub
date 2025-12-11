/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { CountBadge } from '@app/homeV2/content/tabs/CountBadge';

const Tab = styled.div<{ selected: boolean; disabled: boolean }>`
    font-size: 14px;
    line-height: 22px;
    padding: 10px 16px;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    ${(props) => !props.selected && `color: ${ANTD_GRAY[9]};`}
    ${(props) => props.disabled && `color: ${ANTD_GRAY[6]};`}
    ${(props) => props.selected && `background-color: ${props.theme.styles['primary-color']};`}
    ${(props) => props.selected && 'color: #ffffff;'}
    ${(props) =>
        !props.disabled &&
        `:hover {
            cursor: pointer;
            ${!props.selected && `color: ${props.theme.styles['primary-color']};`}
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
