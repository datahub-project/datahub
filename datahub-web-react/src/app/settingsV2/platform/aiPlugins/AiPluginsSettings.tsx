import { SettingOutlined } from '@ant-design/icons';
import { Empty, Spin, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import OAuthServerCard from '@app/settingsV2/platform/aiPlugins/OAuthServerCard';
import OAuthServerFormModal from '@app/settingsV2/platform/aiPlugins/OAuthServerFormModal';
import ServiceCard from '@app/settingsV2/platform/aiPlugins/ServiceCard';
import ServiceFormModal from '@app/settingsV2/platform/aiPlugins/ServiceFormModal';
import { Button, colors } from '@src/alchemy-components';

import {
    useDeleteOAuthAuthorizationServerMutation,
    useDeleteServiceMutation,
    useGetAiPluginsQuery,
    useListOAuthAuthorizationServersQuery,
} from '@graphql/aiPlugins.generated';
import { AiPluginType } from '@types';

const Container = styled.div`
    margin-top: 24px;
`;

const SectionHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 16px;
`;

const SectionTitle = styled.div`
    font-size: 16px;
    font-weight: 700;
    color: ${colors.gray[600]};
`;

const SectionDescription = styled.div`
    color: ${colors.gray[1700]};
    font-size: 14px;
    font-weight: 400;
    line-height: 1.5;
    margin-bottom: 16px;
`;

const StyledCard = styled.div`
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px;
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
    padding: 16px;
    margin-bottom: 16px;
`;

const CardGrid = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
    gap: 16px;
`;

const Divider = styled.hr`
    border: none;
    border-top: 1px solid ${colors.gray[100]};
    margin: 32px 0;
`;

/**
 * AI Plugins Settings component for managing Services (MCP servers) and OAuth Authorization Servers.
 */
export const AiPluginsSettings: React.FC = () => {
    const [isServiceModalOpen, setIsServiceModalOpen] = useState(false);
    const [isOAuthServerModalOpen, setIsOAuthServerModalOpen] = useState(false);
    const [editingPlugin, setEditingPlugin] = useState<any | null>(null);
    const [editingOAuthServerUrn, setEditingOAuthServerUrn] = useState<string | null>(null);

    // Fetch AI plugins from GlobalSettings
    const { data: aiPluginsData, loading: loadingAiPlugins, refetch: refetchAiPlugins } = useGetAiPluginsQuery();

    // Fetch OAuth servers
    const {
        data: oauthServersData,
        loading: loadingOAuthServers,
        refetch: refetchOAuthServers,
    } = useListOAuthAuthorizationServersQuery({
        variables: {
            input: {
                start: 0,
                count: 100,
                query: '',
            },
        },
    });

    const [deleteService] = useDeleteServiceMutation();
    const [deleteOAuthServer] = useDeleteOAuthAuthorizationServerMutation();

    const aiPlugins = aiPluginsData?.globalSettings?.aiPlugins || [];
    const oauthServers = oauthServersData?.listOAuthAuthorizationServers?.authorizationServers || [];
    const mcpServerPlugins = aiPlugins.filter((p) => p.type === AiPluginType.McpServer);

    const handleAddService = () => {
        setEditingPlugin(null);
        setIsServiceModalOpen(true);
    };

    const handleEditService = (plugin: any) => {
        setEditingPlugin(plugin);
        setIsServiceModalOpen(true);
    };

    const handleDeleteService = async (urn: string) => {
        try {
            await deleteService({ variables: { urn } });
            message.success('Service deleted successfully');
            refetchAiPlugins();
        } catch (error) {
            message.error('Failed to delete service');
        }
    };

    const handleAddOAuthServer = () => {
        setEditingOAuthServerUrn(null);
        setIsOAuthServerModalOpen(true);
    };

    const handleEditOAuthServer = (urn: string) => {
        setEditingOAuthServerUrn(urn);
        setIsOAuthServerModalOpen(true);
    };

    const handleDeleteOAuthServer = async (urn: string) => {
        try {
            await deleteOAuthServer({ variables: { urn } });
            message.success('OAuth server deleted successfully');
            refetchOAuthServers();
        } catch (error) {
            message.error('Failed to delete OAuth server');
        }
    };

    const handleServiceModalClose = () => {
        setIsServiceModalOpen(false);
        setEditingPlugin(null);
        refetchAiPlugins();
    };

    const handleOAuthServerModalClose = () => {
        setIsOAuthServerModalOpen(false);
        setEditingOAuthServerUrn(null);
        refetchOAuthServers();
    };

    const loading = loadingAiPlugins || loadingOAuthServers;

    if (loading) {
        return (
            <Container>
                <Spin style={{ marginTop: 40 }} />
            </Container>
        );
    }

    return (
        <Container>
            {/* MCP Servers Section */}
            <StyledCard>
                <SectionHeader>
                    <div>
                        <SectionTitle>
                            <SettingOutlined style={{ marginRight: 8 }} />
                            MCP Servers
                        </SectionTitle>
                    </div>
                    <Button icon={{ icon: 'Add', source: 'material' }} onClick={handleAddService}>
                        Add MCP Server
                    </Button>
                </SectionHeader>
                <SectionDescription>
                    Configure Model Context Protocol (MCP) servers that Ask DataHub can use to access external tools and
                    data sources. MCP servers enable the AI assistant to interact with services like Glean, Slack, and
                    other integrations.
                </SectionDescription>
                {mcpServerPlugins.length === 0 ? (
                    <Empty description="No MCP servers configured yet" />
                ) : (
                    <CardGrid>
                        {mcpServerPlugins.map((plugin) => (
                            <ServiceCard
                                key={plugin.serviceUrn}
                                plugin={plugin}
                                onEdit={() => handleEditService(plugin)}
                                onDelete={() => handleDeleteService(plugin.serviceUrn)}
                            />
                        ))}
                    </CardGrid>
                )}
            </StyledCard>

            <Divider />

            {/* OAuth Authorization Servers Section */}
            <StyledCard>
                <SectionHeader>
                    <div>
                        <SectionTitle>
                            <SettingOutlined style={{ marginRight: 8 }} />
                            OAuth Authorization Servers
                        </SectionTitle>
                    </div>
                    <Button icon={{ icon: 'Add', source: 'material' }} onClick={handleAddOAuthServer}>
                        Add OAuth Server
                    </Button>
                </SectionHeader>
                <SectionDescription>
                    Configure OAuth 2.0 authorization servers for user authentication with external services. Users will
                    be able to connect their accounts to access MCP servers and other integrations using their own
                    credentials.
                </SectionDescription>
                {oauthServers.length === 0 ? (
                    <Empty description="No OAuth servers configured yet" />
                ) : (
                    <CardGrid>
                        {oauthServers.map((server) => (
                            <OAuthServerCard
                                key={server.urn}
                                server={server}
                                onEdit={() => handleEditOAuthServer(server.urn)}
                                onDelete={() => handleDeleteOAuthServer(server.urn)}
                            />
                        ))}
                    </CardGrid>
                )}
            </StyledCard>

            {/* Modals */}
            {isServiceModalOpen && <ServiceFormModal editingPlugin={editingPlugin} onClose={handleServiceModalClose} />}
            {isOAuthServerModalOpen && (
                <OAuthServerFormModal editingUrn={editingOAuthServerUrn} onClose={handleOAuthServerModalClose} />
            )}
        </Container>
    );
};

export default AiPluginsSettings;
