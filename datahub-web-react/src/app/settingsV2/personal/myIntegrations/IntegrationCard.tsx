import { DotsThreeVertical } from '@phosphor-icons/react';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { Menu } from '@components/components/Menu/Menu';
import { ItemType } from '@components/components/Menu/types';

import { getAuthTypeLabel, requiresUserConnection } from '@app/settingsV2/personal/myIntegrations/utils/authTypeUtils';
import { PluginLogo } from '@app/settingsV2/platform/ai/plugins/components/PluginLogo';
import { Button, Pill, Switch, colors } from '@src/alchemy-components';

import { AiPluginAuthType } from '@types';

interface PluginForCard {
    id: string;
    authType: AiPluginAuthType;
    service?: {
        properties?: {
            displayName?: string | null;
            description?: string | null;
        } | null;
        mcpServerProperties?: {
            url?: string | null;
        } | null;
    } | null;
}

const StyledCard = styled.div`
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
    padding: 16px;
`;

const CardHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
`;

const ContentContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 12px;
    flex: 1;
`;

const TextContainer = styled.div`
    display: flex;
    flex-direction: column;
    flex: 1;
`;

const TitleRow = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const SettingText = styled.div`
    font-size: 16px;
    color: ${colors.gray[600]};
    font-weight: 700;
`;

const DescriptionText = styled.div`
    color: ${colors.gray[1700]};
    font-size: 14px;
    font-weight: 400;
    line-height: 1.5;
`;

const ActionsContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    flex-shrink: 0;
`;

interface IntegrationCardProps {
    plugin: PluginForCard & { isConnected: boolean; isEnabled: boolean };
    isConnected: boolean;
    isEnabled: boolean;
    onConnect: () => void;
    onToggleEnabled: (enabled: boolean) => void;
    onDisconnect?: () => void;
    onCustomHeaders?: () => void;
    isConnecting: boolean;
    isToggling: boolean;
    hasCustomHeaders?: boolean;
}

const IntegrationCard: React.FC<IntegrationCardProps> = ({
    plugin,
    isConnected,
    isEnabled,
    onConnect,
    onToggleEnabled,
    onDisconnect,
    onCustomHeaders,
    isConnecting,
    isToggling,
    hasCustomHeaders = false,
}) => {
    const displayName = plugin.service?.properties?.displayName || 'Unknown Plugin';
    const description = plugin.service?.properties?.description;
    const url = plugin.service?.mcpServerProperties?.url || null;
    const { authType } = plugin;

    const needsUserConnection = requiresUserConnection(authType);
    const showToggle = needsUserConnection ? isConnected : true;
    const showConnectButton = needsUserConnection && !isConnected;
    const authTypeLabel = getAuthTypeLabel(authType);

    // Build menu items for the kebab menu
    const menuItems = useMemo(() => {
        const items: ItemType[] = [];

        // Custom Headers - available for plugins that support user configuration
        if (onCustomHeaders) {
            items.push({
                type: 'item' as const,
                key: 'headers',
                title: hasCustomHeaders ? 'Custom Headers (configured)' : 'Custom Headers',
                icon: 'Code' as const,
                onClick: onCustomHeaders,
            });
        }

        // Disconnect - only if connected and has user connection
        if (needsUserConnection && isConnected && onDisconnect) {
            if (items.length > 0) {
                items.push({
                    type: 'divider' as const,
                    key: 'divider',
                });
            }
            items.push({
                type: 'item' as const,
                key: 'disconnect',
                title: 'Disconnect',
                icon: 'LinkBreak' as const,
                danger: true,
                onClick: onDisconnect,
            });
        }

        return items;
    }, [onCustomHeaders, hasCustomHeaders, needsUserConnection, isConnected, onDisconnect]);

    const showMenu = menuItems.length > 0;

    return (
        <StyledCard data-testid={`integration-card-${plugin.id}`}>
            <CardHeader>
                <ContentContainer>
                    <PluginLogo displayName={displayName} url={url} />
                    <TextContainer>
                        <TitleRow>
                            <SettingText>{displayName}</SettingText>
                            <Pill label={authTypeLabel} color="violet" size="sm" variant="filled" />
                        </TitleRow>
                        {description && <DescriptionText>{description}</DescriptionText>}
                    </TextContainer>
                </ContentContainer>
                <ActionsContainer>
                    {showConnectButton && (
                        <Button
                            variant="filled"
                            onClick={onConnect}
                            disabled={isConnecting}
                            isLoading={isConnecting}
                            icon={{ icon: 'Link', source: 'material' }}
                            data-testid={`connect-button-${plugin.id}`}
                        >
                            {isConnecting ? 'Connecting...' : 'Connect'}
                        </Button>
                    )}
                    {showToggle && (
                        <Switch
                            label=""
                            isChecked={isEnabled}
                            isDisabled={isToggling}
                            onChange={(e) => {
                                e.stopPropagation();
                                onToggleEnabled(e.target.checked);
                            }}
                            data-testid={`toggle-switch-${plugin.id}`}
                        />
                    )}
                    {showMenu && (
                        <Menu items={menuItems} trigger={['click']}>
                            <Button variant="text" size="sm" data-testid="plugin-menu-button">
                                <DotsThreeVertical size={20} color={colors.gray[500]} weight="bold" />
                            </Button>
                        </Menu>
                    )}
                </ActionsContainer>
            </CardHeader>
        </StyledCard>
    );
};

export default IntegrationCard;
