import React, { useState } from 'react';
import styled from 'styled-components';
import { Menu } from 'antd';
import {
    DeleteOutlined,
    PlaySquareOutlined,
    StopOutlined,
    SettingOutlined,
    PlusOutlined,
    MinusOutlined,
    LinkOutlined,
    CheckOutlined,
    CopyOutlined,
} from '@ant-design/icons';
import { Monitor } from '../../../../../../types.generated';
import { isMonitorActive } from './acrylUtils';
import { useAppConfig } from '../../../../../useAppConfig';
import { useAssertionURNCopyLink } from './assertion/builder/hooks';
import { useIsSeparateSiblingsMode } from '../../../siblingUtils';

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

const TextSpan = styled.span`
    padding-left: 7px;
`;

const StyledLinkOutlined = styled(LinkOutlined)`
    margin-right: 8px;
`;
const StyledCheckOutlined = styled(CheckOutlined)`
    margin-right: 8px;
`;
const StyledCopyOutlined = styled(CopyOutlined)`
    margin-right: 8px;
`;

type Props = {
    urn: string;
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
    urn,
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
    const isSeparateSiblingsMode = useIsSeparateSiblingsMode();
    const contractsEnabled = isSeparateSiblingsMode && appConfig.config.featureFlags?.dataContractsEnabled;
    const [isUrnCopied, setIsUrnCopied] = useState(false);

    // To handle copying assertion URN link
    const onCopyLink = useAssertionURNCopyLink(urn);

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
            <Menu.Item key="4" onClick={onCopyLink}>
                <StyledLinkOutlined />
                <TextSpan>Copy Link</TextSpan>
            </Menu.Item>
            <Menu.Item
                key="5"
                onClick={() => {
                    navigator.clipboard.writeText(urn);
                    setIsUrnCopied(true);
                }}
            >
                {isUrnCopied ? <StyledCheckOutlined /> : <StyledCopyOutlined />}
                <TextSpan>Copy URN</TextSpan>
            </Menu.Item>
        </Menu>
    );
};
