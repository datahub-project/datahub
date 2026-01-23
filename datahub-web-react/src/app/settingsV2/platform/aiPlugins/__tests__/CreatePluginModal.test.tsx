import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import CreatePluginModal from '@app/settingsV2/platform/aiPlugins/CreatePluginModal';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { AiPluginAuthType, AiPluginType, McpTransport } from '@src/types.generated';

const mockOnClose = vi.fn();

/**
 * Test wrapper that provides theme context for Alchemy components.
 */
const TestWrapper: React.FC<{ children: React.ReactNode; mocks?: any[] }> = ({ children, mocks = [] }) => (
    <MockedProvider mocks={mocks} addTypename={false}>
        <CustomThemeProvider>{children}</CustomThemeProvider>
    </MockedProvider>
);

const mockEditingPlugin = {
    id: 'plugin-1',
    type: AiPluginType.McpServer,
    serviceUrn: 'urn:li:service:plugin-1',
    enabled: true,
    instructions: 'Test instructions',
    authType: AiPluginAuthType.None,
    service: {
        urn: 'urn:li:service:plugin-1',
        type: 'SERVICE',
        subType: 'MCP_SERVER',
        properties: {
            displayName: 'Existing Plugin',
            description: 'An existing plugin description',
        },
        mcpServerProperties: {
            url: 'https://example.com/mcp',
            transport: McpTransport.Http,
            timeout: 30,
        },
    },
    oauthConfig: null,
    sharedApiKeyConfig: null,
    userApiKeyConfig: null,
};

describe('CreatePluginModal', () => {
    beforeEach(() => {
        mockOnClose.mockClear();
    });

    it('renders in create mode with correct title', async () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        expect(screen.getByText('Create AI Plugin')).toBeInTheDocument();
        expect(screen.getByText('Add an MCP server that can be accessed by Ask DataHub')).toBeInTheDocument();
        expect(screen.getByText('Create')).toBeInTheDocument();
    });

    it('renders in edit mode with correct title and pre-populated data', async () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={mockEditingPlugin as any} onClose={mockOnClose} />
            </TestWrapper>,
        );

        expect(screen.getByText('Edit AI Plugin')).toBeInTheDocument();
        expect(screen.getByText('Save')).toBeInTheDocument();

        // Check that form is pre-populated
        await waitFor(() => {
            const nameInput = screen.getByDisplayValue('Existing Plugin');
            expect(nameInput).toBeInTheDocument();
        });
    });

    it('shows required field sections', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        // Check section titles
        expect(screen.getByText('Connection')).toBeInTheDocument();
        expect(screen.getByText('Authentication')).toBeInTheDocument();
        expect(screen.getByText('LLM Instructions')).toBeInTheDocument();
    });

    it('shows transport options', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        expect(screen.getByText('HTTP / SSE')).toBeInTheDocument();
        expect(screen.getByText('WebSocket')).toBeInTheDocument();
    });

    it('shows enable checkbox in footer', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        expect(screen.getByText('Enable for Ask DataHub')).toBeInTheDocument();
    });

    it('calls onClose when Cancel is clicked', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        fireEvent.click(screen.getByText('Cancel'));
        expect(mockOnClose).toHaveBeenCalled();
    });

    it('shows advanced settings toggle', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        expect(screen.getByText('Advanced Settings')).toBeInTheDocument();
    });

    it('expands advanced settings when clicked', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        // Click to expand
        fireEvent.click(screen.getByText('Advanced Settings'));

        // Should show custom headers section
        expect(screen.getByText('Custom Headers')).toBeInTheDocument();
        expect(screen.getByText('Add Header')).toBeInTheDocument();
    });

    it('renders in duplicate mode with correct title when plugin has no URN', async () => {
        // Create a plugin without a URN (simulates duplication)
        const duplicatedPlugin = {
            ...mockEditingPlugin,
            id: '',
            service: {
                ...mockEditingPlugin.service,
                urn: '', // No URN = duplicate mode
                properties: {
                    ...mockEditingPlugin.service.properties,
                    displayName: 'Existing Plugin (Copy)',
                },
            },
        };

        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={duplicatedPlugin as any} onClose={mockOnClose} />
            </TestWrapper>,
        );

        expect(screen.getByText('Duplicate AI Plugin')).toBeInTheDocument();
        expect(screen.getByText('Create')).toBeInTheDocument(); // Button should say Create, not Save

        // Check that form is pre-populated with copied data
        await waitFor(() => {
            const nameInput = screen.getByDisplayValue('Existing Plugin (Copy)');
            expect(nameInput).toBeInTheDocument();
        });
    });

    it('shows error when creating plugin with duplicate name', async () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal
                    editingPlugin={null}
                    onClose={mockOnClose}
                    existingNames={['Test Plugin', 'Another Plugin']}
                />
            </TestWrapper>,
        );

        // Enter a duplicate name
        const nameInput = screen.getByPlaceholderText('e.g., Glean MCP Server');
        fireEvent.change(nameInput, { target: { value: 'Test Plugin' } });

        // Enter required URL
        const urlInput = screen.getByPlaceholderText('https://api.example.com/mcp');
        fireEvent.change(urlInput, { target: { value: 'https://example.com' } });

        // Click Create - button should NOT be disabled
        const createButton = screen.getByText('Create');
        expect(createButton.closest('button')).not.toBeDisabled();
        fireEvent.click(createButton);

        // Should show duplicate name error on the field
        await waitFor(() => {
            expect(screen.getByText('A plugin with this name already exists')).toBeInTheDocument();
        });
    });

    it('allows creating plugin with unique name', async () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal
                    editingPlugin={null}
                    onClose={mockOnClose}
                    existingNames={['Test Plugin', 'Another Plugin']}
                />
            </TestWrapper>,
        );

        // Enter a unique name
        const nameInput = screen.getByPlaceholderText('e.g., Glean MCP Server');
        fireEvent.change(nameInput, { target: { value: 'Unique Plugin Name' } });

        // Enter required URL
        const urlInput = screen.getByPlaceholderText('https://api.example.com/mcp');
        fireEvent.change(urlInput, { target: { value: 'https://example.com' } });

        // Create button should always be enabled (validation happens on submit)
        const createButton = screen.getByText('Create');
        expect(createButton.closest('button')).not.toBeDisabled();
    });
});
