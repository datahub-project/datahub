import { ArrowUpRight, Plug } from '@phosphor-icons/react';
import React, { useMemo } from 'react';
import styled, { keyframes } from 'styled-components';

import { Menu } from '@components/components/Menu/Menu';
import { ItemType } from '@components/components/Menu/types';
import { getColor } from '@components/theme/utils';

import { usePluginMenuHandlers } from '@app/chat/hooks/usePluginMenuHandlers';
import { getEmptyStateMessage, getPluginDisplayInfo, shouldShowPluginAnimation } from '@app/chat/utils/pluginMenuUtils';
import { useUserContext } from '@app/context/useUserContext';
import ApiKeyModal from '@app/settingsV2/personal/aiConnections/ApiKeyModal';
import { mergePluginsWithUserConfig } from '@app/settingsV2/personal/myIntegrations/utils/pluginDataMapper';
import { PluginLogo } from '@app/settingsV2/platform/ai/plugins/components/PluginLogo';
import useHasSeenEducationStep from '@providers/hooks/useHasSeenEducationStep';
import { Button, Switch, Text, colors } from '@src/alchemy-components';

import { useGetAiPluginsWithUserStatusQuery } from '@graphql/aiPlugins.generated';

const PLUGIN_MENU_EDUCATION_STEP_ID = 'chat-plugins-menu';

const PluginRow = styled.div`
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 4px 8px;
    min-width: 200px;
`;

const PluginInfo = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    flex: 1;
`;

const ManageRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 4px 8px;
    min-width: 200px;
`;

const EmptyStateContainer = styled.div`
    padding: 16px;
    min-width: 280px;
    max-width: 320px;
    text-align: center;
`;

const EmptyStateTextRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
    flex-wrap: wrap;
`;

const EmptyStateActions = styled.div`
    display: flex;
    justify-content: center;
`;

// Animated gradient border components
const GradientAnimation = keyframes`
    0% {
        transform: rotate(0deg);
    }
    100% {
        transform: rotate(360deg);
    }
`;

const PluginButtonWrapper = styled.div<{ $showAnimation: boolean }>`
    position: relative;
    border-radius: 8px;
    display: flex;
    ${(props) => (props.$showAnimation ? 'padding: 1px;' : '')}
`;

const AnimatedGradientBorder = styled.div`
    position: absolute;
    inset: 0;
    border-radius: 8px;
    overflow: hidden;
    z-index: 0;

    ::before {
        content: '';
        position: absolute;
        width: 300%;
        height: 300%;
        top: -100%;
        left: -100%;
        background: conic-gradient(
            from 0deg,
            transparent 0%,
            transparent 60%,
            ${colors.violet[500]} 70%,
            ${colors.violet[400]} 80%,
            ${colors.violet[200]} 90%,
            ${colors.violet[0]} 100%
        );
        animation: ${GradientAnimation} 2s linear infinite;
    }
`;

const ButtonContentWrapper = styled.div<{ $showAnimation: boolean }>`
    position: relative;
    z-index: 1;
    display: flex;
    background: ${colors.white};
    border-radius: 6px;
`;

const PlugIconWrapper = styled.div<{ $useGradient: boolean }>`
    display: flex;
    align-items: center;
    justify-content: center;

    ${({ $useGradient }) =>
        $useGradient
            ? `
        && svg {
            fill: url(#plugin-icon-gradient);
        }
    `
            : ''}
`;

export const ChatPluginsMenu: React.FC = () => {
    const me = useUserContext();
    const { data, refetch } = useGetAiPluginsWithUserStatusQuery({
        fetchPolicy: 'cache-and-network',
    });

    // Track if user has seen the plugins menu
    const hasSeenPluginsMenu = useHasSeenEducationStep(PLUGIN_MENU_EDUCATION_STEP_ID);

    const availablePlugins = useMemo(() => {
        const globalPlugins = (data?.globalSettings?.aiPlugins || []) as Parameters<
            typeof mergePluginsWithUserConfig
        >[0];
        const userPlugins = (data?.me?.corpUser?.settings?.aiPluginSettings?.plugins || []) as Parameters<
            typeof mergePluginsWithUserConfig
        >[1];

        return mergePluginsWithUserConfig(globalPlugins, userPlugins);
    }, [data]);

    // Only show admin actions if user has manageGlobalSettings privilege
    const isAdmin = (me && me.platformPrivileges?.manageGlobalSettings) || false;

    // Extract all handlers and state management to custom hook
    const {
        isOpen,
        setIsOpen,
        togglingPluginId,
        apiKeyModal,
        setApiKeyModal,
        connectingPluginId,
        handleConnect,
        handleApiKeySubmit,
        handleToggleEnabled,
        handleManageIntegrations,
        handleOpenChange,
        handleCreatePlugin,
    } = usePluginMenuHandlers({
        refetch,
        hasSeenPluginsMenu,
        availablePluginsCount: availablePlugins.length,
        educationStepId: PLUGIN_MENU_EDUCATION_STEP_ID,
    });

    const menuItems: ItemType[] = useMemo(() => {
        const items: ItemType[] = [];

        // Show empty state if no plugins available
        if (availablePlugins.length === 0) {
            items.push({
                type: 'item' as const,
                key: 'empty',
                title: 'No AI plugins available',
                render: () => (
                    <EmptyStateContainer>
                        <EmptyStateTextRow>
                            <Text size="sm" color="gray" colorLevel={600}>
                                {getEmptyStateMessage(isAdmin)}
                            </Text>
                            <Button
                                variant="text"
                                size="sm"
                                onClick={() => {
                                    setIsOpen(false);
                                    // TODO: Add link to documentation when available
                                }}
                            >
                                Learn More
                            </Button>
                        </EmptyStateTextRow>
                        {isAdmin ? (
                            <EmptyStateActions>
                                <Button variant="filled" size="sm" onClick={handleCreatePlugin}>
                                    Create AI Plugin
                                </Button>
                            </EmptyStateActions>
                        ) : null}
                    </EmptyStateContainer>
                ),
            });
            return items;
        }

        // Show all plugins (connected and unconnected)
        availablePlugins.forEach((plugin) => {
            const { displayName, url, showConnectButton, showToggle } = getPluginDisplayInfo(plugin);

            items.push({
                type: 'item' as const,
                key: plugin.id,
                title: displayName,
                render: () => (
                    <PluginRow onClick={(e) => e.stopPropagation()}>
                        <PluginInfo>
                            <PluginLogo displayName={displayName} url={url} />
                            <Text weight="semiBold" color="gray" colorLevel={600}>
                                {displayName}
                            </Text>
                        </PluginInfo>
                        {showConnectButton && (
                            <Button
                                variant="filled"
                                size="xs"
                                onClick={(e) => {
                                    e.stopPropagation();
                                    handleConnect(plugin.id, displayName, plugin.authType);
                                }}
                                isLoading={connectingPluginId === plugin.id}
                                disabled={connectingPluginId === plugin.id}
                            >
                                {connectingPluginId === plugin.id ? 'Connecting...' : 'Connect'}
                            </Button>
                        )}
                        {showToggle && (
                            <Switch
                                label=""
                                isChecked={plugin.isEnabled}
                                isDisabled={togglingPluginId === plugin.id}
                                onChange={(e) => {
                                    e.stopPropagation();
                                    handleToggleEnabled(plugin.id, e.target.checked);
                                }}
                            />
                        )}
                    </PluginRow>
                ),
            });
        });

        // Add divider and manage link
        items.push({
            type: 'divider' as const,
            key: 'divider',
        });

        items.push({
            type: 'item' as const,
            key: 'manage',
            title: 'Manage AI plugins',
            onClick: handleManageIntegrations,
            render: () => (
                <ManageRow>
                    <Text weight="semiBold" color="gray" colorLevel={600}>
                        Manage AI plugins
                    </Text>
                    <ArrowUpRight size={16} color={colors.gray[500]} />
                </ManageRow>
            ),
        });

        return items;
    }, [
        availablePlugins,
        isAdmin,
        togglingPluginId,
        connectingPluginId,
        handleToggleEnabled,
        handleConnect,
        handleManageIntegrations,
        handleCreatePlugin,
        setIsOpen,
    ]);

    // Show animation if user hasn't seen the menu and there are plugins available
    const showAnimation = shouldShowPluginAnimation(hasSeenPluginsMenu, availablePlugins.length);

    return (
        <>
            {/* Hidden SVG for gradient definition */}
            <svg
                style={{ width: 0, height: 0, position: 'absolute', visibility: 'hidden' }}
                aria-hidden="true"
                focusable="false"
            >
                <linearGradient id="plugin-icon-gradient" x2="1" y2="1">
                    <stop offset="1%" stopColor={getColor('primary', 300)} />
                    <stop offset="99%" stopColor={getColor('primary', 500)} />
                </linearGradient>
            </svg>

            <Menu items={menuItems} trigger={['click']} open={isOpen} onOpenChange={handleOpenChange}>
                <PluginButtonWrapper $showAnimation={showAnimation}>
                    {showAnimation ? <AnimatedGradientBorder /> : null}
                    <ButtonContentWrapper $showAnimation={showAnimation}>
                        <Button
                            variant="text"
                            size="sm"
                            title="Turn on plugins to include data & capabilities from 3rd party tools"
                            aria-label="AI plugins menu"
                        >
                            <PlugIconWrapper $useGradient={showAnimation}>
                                <Plug
                                    size={16}
                                    color={showAnimation ? undefined : colors.gray[500]}
                                    weight={showAnimation ? 'fill' : 'regular'}
                                />
                            </PlugIconWrapper>
                        </Button>
                    </ButtonContentWrapper>
                </PluginButtonWrapper>
            </Menu>

            {/* API Key Modal */}
            <ApiKeyModal
                open={apiKeyModal !== null}
                pluginName={apiKeyModal?.pluginName || ''}
                onClose={() => setApiKeyModal(null)}
                onSubmit={handleApiKeySubmit}
            />
        </>
    );
};

export default ChatPluginsMenu;
