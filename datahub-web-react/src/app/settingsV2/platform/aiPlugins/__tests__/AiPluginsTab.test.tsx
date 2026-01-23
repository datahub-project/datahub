import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { AiPluginsTab } from '@app/settingsV2/platform/aiPlugins/AiPluginsTab';
import { GetAiPluginsDocument } from '@src/graphql/aiPlugins.generated';
import { AiPluginAuthType, AiPluginType } from '@src/types.generated';

const mockSetShowCreateModal = vi.fn();

const mockEmptyPlugins = {
    request: {
        query: GetAiPluginsDocument,
    },
    result: {
        data: {
            globalSettings: {
                aiPlugins: [],
            },
        },
    },
};

const mockPlugins = {
    request: {
        query: GetAiPluginsDocument,
    },
    result: {
        data: {
            globalSettings: {
                aiPlugins: [
                    {
                        id: 'plugin-1',
                        type: AiPluginType.McpServer,
                        serviceUrn: 'urn:li:service:plugin-1',
                        enabled: true,
                        instructions: null,
                        authType: AiPluginAuthType.UserOauth,
                        service: {
                            urn: 'urn:li:service:plugin-1',
                            type: 'SERVICE',
                            subType: 'MCP_SERVER',
                            properties: {
                                displayName: 'Test Plugin',
                                description: 'A test plugin',
                            },
                            mcpServerProperties: null,
                        },
                        oauthConfig: null,
                        sharedApiKeyConfig: null,
                        userApiKeyConfig: null,
                    },
                ],
            },
        },
    },
};

describe('AiPluginsTab', () => {
    it('renders empty state when no plugins exist', async () => {
        render(
            <MockedProvider mocks={[mockEmptyPlugins]} addTypename={false}>
                <AiPluginsTab showCreateModal={false} setShowCreateModal={mockSetShowCreateModal} />
            </MockedProvider>,
        );

        // Wait for loading to complete
        expect(await screen.findByText('No plugins yet!')).toBeInTheDocument();
        expect(screen.getByTestId('ai-plugins-tab-empty')).toBeInTheDocument();
    });

    it('renders plugins table when plugins exist', async () => {
        render(
            <MockedProvider mocks={[mockPlugins]} addTypename={false}>
                <AiPluginsTab showCreateModal={false} setShowCreateModal={mockSetShowCreateModal} />
            </MockedProvider>,
        );

        // Wait for the plugin name to appear
        expect(await screen.findByText('Test Plugin')).toBeInTheDocument();
        expect(screen.getByText('OAuth')).toBeInTheDocument();
        expect(screen.getByTestId('ai-plugins-tab')).toBeInTheDocument();
    });
});
