import React from 'react';
import styled from 'styled-components';
import { Menu } from 'antd';
import { DeleteOutlined, PlaySquareOutlined, StopOutlined, SettingOutlined } from '@ant-design/icons';
import { Monitor } from '../../../../../../types.generated';
import { isMonitorActive } from './acrylUtils';

const StyledStopOutlined = styled(StopOutlined)`
    margin-right: 8px;
`;

const StyledSettingOutlined = styled(SettingOutlined)`
    margin-right: 8px;
`;

const StyledPlaySquareOutlined = styled(PlaySquareOutlined)`
    margin-right: 8px;
`;

const StyledDeleteOutlined = styled(DeleteOutlined)`
    margin-right: 8px;
`;

type Props = {
    monitor?: Monitor;
    canManageAssertion: boolean;
    onManageAssertion: () => void;
    onDeleteAssertion: () => void;
    onStopMonitor?: () => void;
    onStartMonitor?: () => void;
};

export const AssertionActionsMenu = ({
    monitor,
    canManageAssertion,
    onManageAssertion,
    onDeleteAssertion,
    onStopMonitor,
    onStartMonitor,
}: Props) => {
    return (
        <Menu>
            {(monitor &&
                (isMonitorActive(monitor) ? (
                    <Menu.Item key="0" onClick={onStopMonitor} disabled={!canManageAssertion}>
                        <StyledStopOutlined />
                        Stop
                    </Menu.Item>
                ) : (
                    <Menu.Item key="0" onClick={onStartMonitor} disabled={!canManageAssertion}>
                        <StyledPlaySquareOutlined />
                        Start
                    </Menu.Item>
                ))) ||
                undefined}
            <Menu.Item key="1" onClick={onManageAssertion}>
                <StyledSettingOutlined />
                Manage...
            </Menu.Item>
            <Menu.Item key="2" onClick={onDeleteAssertion}>
                <StyledDeleteOutlined />
                Delete
            </Menu.Item>
        </Menu>
    );
};
