import React from 'react';
import styled from 'styled-components';
import { Menu } from 'antd';
import {
    DeleteOutlined,
    PlaySquareOutlined,
    StopOutlined,
    SettingOutlined,
    PlusOutlined,
    MinusOutlined,
} from '@ant-design/icons';
import { Monitor } from '../../../../../../types.generated';
import { isMonitorActive } from './acrylUtils';
import { useAppConfig } from '../../../../../useAppConfig';

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

const StyledMinusOutlined = styled(MinusOutlined)`
    margin-right: 8px;
`;

const StyledPlusOutlined = styled(PlusOutlined)`
    margin-right: 8px;
`;

type Props = {
    monitor?: Monitor;
    canManageAssertion: boolean;
    isPartOfContract?: boolean;
    onManageAssertion: () => void;
    onDeleteAssertion: () => void;
    onStopMonitor?: () => void;
    onStartMonitor?: () => void;
    onAddToContract?: () => void;
    onRemoveFromContract?: () => void;
};

export const AssertionActionsMenu = ({
    monitor,
    canManageAssertion,
    isPartOfContract = false,
    onManageAssertion,
    onDeleteAssertion,
    onStopMonitor,
    onStartMonitor,
    onAddToContract,
    onRemoveFromContract,
}: Props) => {
    const appConfig = useAppConfig();
    const contractsEnabled = appConfig.config.featureFlags?.dataContractsEnabled;
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
            {(contractsEnabled &&
                (isPartOfContract ? (
                    <Menu.Item key="3" onClick={onRemoveFromContract}>
                        <StyledMinusOutlined />
                        Remove from contract
                    </Menu.Item>
                ) : (
                    <Menu.Item key="3" onClick={onAddToContract}>
                        <StyledPlusOutlined />
                        Add to contract
                    </Menu.Item>
                ))) ||
                undefined}
        </Menu>
    );
};
