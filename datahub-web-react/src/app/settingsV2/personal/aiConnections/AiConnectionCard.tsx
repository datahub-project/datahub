import { CheckCircleFilled, CheckOutlined } from '@ant-design/icons';
import { Switch, Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { Button, colors } from '@src/alchemy-components';

import { AiPluginAuthType, AiPluginConfig } from '@types';

const Card = styled.div`
    border: 1px solid ${colors.gray[100]};
    border-radius: 8px;
    padding: 16px;
    background: white;
    transition: box-shadow 0.2s;

    &:hover {
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }
`;

const CardHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 12px;
`;

const PluginInfo = styled.div`
    flex: 1;
`;

const PluginName = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
    margin-bottom: 4px;
`;

const PluginDescription = styled.div`
    font-size: 12px;
    color: ${colors.gray[500]};
    line-height: 1.4;
`;

const StatusContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 12px;
`;

const StatusBadge = styled.span<{ $variant: 'gray' | 'green' | 'yellow' | 'blue' }>`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    font-size: 12px;
    padding: 4px 8px;
    border-radius: 12px;
    background: ${(props) => {
        switch (props.$variant) {
            case 'green':
                return colors.green[100];
            case 'yellow':
                return colors.yellow[100];
            case 'blue':
                return colors.blue[100];
            default:
                return colors.gray[100];
        }
    }};
    color: ${(props) => {
        switch (props.$variant) {
            case 'green':
                return colors.green[700];
            case 'yellow':
                return colors.yellow[700];
            case 'blue':
                return colors.blue[700];
            default:
                return colors.gray[600];
        }
    }};
`;

const AuthTypeBadge = styled.span`
    font-size: 10px;
    padding: 2px 6px;
    border-radius: 4px;
    background: ${colors.violet[100]};
    color: ${colors.violet[600]};
    text-transform: uppercase;
`;

const ActionsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
    margin-top: 12px;
`;

const EnableToggle = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 13px;
    color: ${colors.gray[600]};
`;

const DisconnectLink = styled.button`
    background: none;
    border: none;
    padding: 0;
    font-size: 11px;
    color: ${colors.gray[400]};
    cursor: pointer;
    text-decoration: underline;
    margin-left: auto;

    &:hover {
        color: ${colors.red[500]};
    }
`;

interface AiConnectionCardProps {
    /** The AI plugin configuration from global settings */
    plugin: AiPluginConfig & { isConnected: boolean; isEnabled: boolean };
    /** Whether the user has connected their account (for USER_OAUTH/USER_API_KEY) */
    isConnected: boolean;
    /** Whether the plugin is enabled for the user */
    isEnabled: boolean;
    /** Callback when user clicks Connect (for USER_OAUTH/USER_API_KEY) */
    onConnect: () => void;
    /** Callback when user toggles enable/disable */
    onToggleEnabled: (enabled: boolean) => void;
    /** Callback when user clicks Disconnect (optional, for testing/advanced users) */
    onDisconnect?: () => void;
    /** Whether a connection is in progress */
    isConnecting: boolean;
    /** Whether an enable/disable toggle is in progress */
    isToggling: boolean;
}

/**
 * Card component displaying an AI plugin and its connection/enable status.
 *
 * For USER_OAUTH / USER_API_KEY plugins:
 * - Disconnected: Shows "Not Connected" + Connect button
 * - Connected + Enabled: Shows "Connected" + enable toggle (on)
 * - Connected + Disabled: Shows "Connected (Disabled)" + enable toggle (off)
 *
 * For SHARED_API_KEY / NONE plugins:
 * - Shows "Ready to use" status (no connection needed)
 * - Enable/disable toggle (disabled by default)
 */
const AiConnectionCard: React.FC<AiConnectionCardProps> = ({
    plugin,
    isConnected,
    isEnabled,
    onConnect,
    onToggleEnabled,
    onDisconnect,
    isConnecting,
    isToggling,
}) => {
    const displayName = plugin.service?.properties?.displayName || 'Unknown Plugin';
    const description = plugin.service?.properties?.description;
    const { authType } = plugin;

    // Check if this plugin requires user connection
    const requiresUserConnection = authType === AiPluginAuthType.UserOauth || authType === AiPluginAuthType.UserApiKey;

    // Get auth type label for the badge
    const getAuthTypeLabel = () => {
        switch (authType) {
            case AiPluginAuthType.UserOauth:
                return 'OAuth';
            case AiPluginAuthType.UserApiKey:
                return 'API Key';
            case AiPluginAuthType.SharedApiKey:
                return 'Shared Key';
            case AiPluginAuthType.None:
                return 'Public';
            default:
                return '';
        }
    };

    // Get status text and variant based on state
    const getStatusInfo = (): {
        text: string;
        variant: 'gray' | 'green' | 'yellow' | 'blue';
        icon?: React.ReactNode;
    } => {
        if (requiresUserConnection) {
            if (!isConnected) {
                return { text: 'Not Connected', variant: 'gray' };
            }
            return isEnabled
                ? { text: 'Connected', variant: 'green', icon: <CheckCircleFilled /> }
                : { text: 'Connected (Disabled)', variant: 'yellow', icon: <CheckCircleFilled /> };
        }
        // SHARED_API_KEY or NONE - no connection needed
        return isEnabled
            ? { text: 'Ready to use', variant: 'blue', icon: <CheckOutlined /> }
            : { text: 'Available', variant: 'gray' };
    };

    const statusInfo = getStatusInfo();

    // Determine if we should show the toggle
    // For USER_* types: only after connected
    // For SHARED_*/NONE: always show toggle
    const showToggle = requiresUserConnection ? isConnected : true;

    // Determine if we should show the connect button
    const showConnectButton = requiresUserConnection && !isConnected;

    return (
        <Card>
            <CardHeader>
                <PluginInfo>
                    <PluginName>{displayName}</PluginName>
                    {description && <PluginDescription>{description}</PluginDescription>}
                </PluginInfo>
            </CardHeader>

            <StatusContainer>
                <StatusBadge $variant={statusInfo.variant}>
                    {statusInfo.icon}
                    {statusInfo.text}
                </StatusBadge>
                <Tooltip title={`Authentication: ${getAuthTypeLabel()}`}>
                    <AuthTypeBadge>{getAuthTypeLabel()}</AuthTypeBadge>
                </Tooltip>
            </StatusContainer>

            <ActionsContainer>
                {showToggle && (
                    <EnableToggle>
                        <Switch checked={isEnabled} onChange={onToggleEnabled} loading={isToggling} size="small" />
                        <span>{isEnabled ? 'Enabled' : 'Disabled'}</span>
                    </EnableToggle>
                )}
                {showConnectButton && (
                    <Button
                        variant="filled"
                        onClick={onConnect}
                        disabled={isConnecting}
                        isLoading={isConnecting}
                        icon={{ icon: 'Link', source: 'material' }}
                    >
                        {isConnecting ? 'Connecting...' : 'Connect'}
                    </Button>
                )}
                {requiresUserConnection && isConnected && onDisconnect && (
                    <DisconnectLink onClick={onDisconnect}>Disconnect</DisconnectLink>
                )}
            </ActionsContainer>
        </Card>
    );
};

export default AiConnectionCard;
