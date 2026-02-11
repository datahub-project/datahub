import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import CreatePluginModal from '@app/settingsV2/platform/ai/plugins/CreatePluginModal';
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
            transport: McpTransport.Sse,
            timeout: 30,
        },
    },
    oauthConfig: null,
    sharedApiKeyConfig: null,
    userApiKeyConfig: null,
};

/** Helper: select the Custom source card to advance to the config step */
function selectCustomSource() {
    const customCard = screen.getByTestId('source-card-custom');
    fireEvent.click(customCard);
}

describe('CreatePluginModal', () => {
    beforeEach(() => {
        mockOnClose.mockClear();
    });

    // ------------------------------------------------------------------
    // Step 1: Source Selection
    // ------------------------------------------------------------------

    it('renders source selection step in create mode', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        expect(screen.getByText('Create AI Plugin')).toBeInTheDocument();
        // Source cards should be visible
        expect(screen.getByTestId('source-card-custom')).toBeInTheDocument();
        expect(screen.getByTestId('source-card-github')).toBeInTheDocument();
        expect(screen.getByTestId('source-card-dbt')).toBeInTheDocument();
        expect(screen.getByTestId('source-card-snowflake')).toBeInTheDocument();
    });

    it('advances to configuration step after selecting Custom source', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        selectCustomSource();

        // Now we should see the config step
        expect(screen.getByTestId('plugin-configuration-step')).toBeInTheDocument();
        expect(screen.getByText('Authentication Type')).toBeInTheDocument();
        expect(screen.getByText('Create')).toBeInTheDocument();
    });

    // ------------------------------------------------------------------
    // Step 2: Configuration (via Custom source)
    // ------------------------------------------------------------------

    it('shows required field sections after selecting Custom', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        selectCustomSource();

        expect(screen.getByTestId('plugin-url-input')).toBeInTheDocument();
        expect(screen.getByText('Authentication Type')).toBeInTheDocument();
    });

    it('shows stdio disclaimer in server URL helper text after selecting Custom', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        selectCustomSource();

        expect(screen.getByText('stdio-based MCP plugins are not currently supported.')).toBeInTheDocument();
    });

    it('shows enable checkbox in footer after selecting Custom', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        selectCustomSource();

        expect(screen.getByText('Enable for Ask DataHub')).toBeInTheDocument();
    });

    it('calls onClose when close icon is clicked on source selection step', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        fireEvent.click(screen.getByTestId('modal-close-icon'));
        expect(mockOnClose).toHaveBeenCalled();
    });

    it('shows advanced settings toggle after selecting Custom', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        selectCustomSource();

        expect(screen.getByText('Advanced Settings')).toBeInTheDocument();
    });

    it('expands advanced settings when clicked', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        selectCustomSource();

        fireEvent.click(screen.getByText('Advanced Settings'));

        expect(screen.getByText('Custom Headers')).toBeInTheDocument();
        expect(screen.getByText('Add Header')).toBeInTheDocument();
    });

    // ------------------------------------------------------------------
    // Edit mode (skips source selection)
    // ------------------------------------------------------------------

    it('renders in edit mode with correct title and pre-populated data', async () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={mockEditingPlugin as any} onClose={mockOnClose} />
            </TestWrapper>,
        );

        expect(screen.getByText('Edit AI Plugin')).toBeInTheDocument();
        expect(screen.getByText('Save')).toBeInTheDocument();

        await waitFor(() => {
            const nameInput = screen.getByDisplayValue('Existing Plugin');
            expect(nameInput).toBeInTheDocument();
        });
    });

    it('renders in duplicate mode with correct title when plugin has no URN', async () => {
        const duplicatedPlugin = {
            ...mockEditingPlugin,
            id: '',
            service: {
                ...mockEditingPlugin.service,
                urn: '',
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
        expect(screen.getByText('Create')).toBeInTheDocument();

        await waitFor(() => {
            const nameInput = screen.getByDisplayValue('Existing Plugin (Copy)');
            expect(nameInput).toBeInTheDocument();
        });
    });

    // ------------------------------------------------------------------
    // Source-specific flows
    // ------------------------------------------------------------------

    it('pre-populates GitHub defaults when GitHub source is selected', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        const githubCard = screen.getByTestId('source-card-github');
        fireEvent.click(githubCard);

        // Should show OAuth credential provider card (GitHub uses UserOauth)
        expect(screen.getByText('Credential Provider')).toBeInTheDocument();
    });

    it('pre-populates dbt defaults when dbt source is selected', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        const dbtCard = screen.getByTestId('source-card-dbt');
        fireEvent.click(dbtCard);

        // dbt uses SharedApiKey, should show API key field
        expect(screen.getByText('Access Token')).toBeInTheDocument();
        // Should show structured headers (Configuration)
        expect(screen.getByText('Configuration')).toBeInTheDocument();
        expect(screen.getByText('Production Environment ID')).toBeInTheDocument();
        expect(screen.getByText('Development Environment ID')).toBeInTheDocument();
        // User ID should be visible for SharedApiKey (default auth type for dbt)
        expect(screen.getByText('User ID')).toBeInTheDocument();
    });

    it('pre-populates Snowflake defaults when Snowflake source is selected', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        const snowflakeCard = screen.getByTestId('source-card-snowflake');
        fireEvent.click(snowflakeCard);

        // Should show OAuth credential provider card (Snowflake uses UserOauth)
        expect(screen.getByText('Credential Provider')).toBeInTheDocument();
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

        selectCustomSource();

        // Enter a duplicate name
        const nameInput = screen.getByTestId('plugin-name-input').querySelector('input')!;
        fireEvent.change(nameInput, { target: { value: 'Test Plugin' } });

        // Enter required URL
        const urlInput = screen.getByTestId('plugin-url-input').querySelector('input')!;
        fireEvent.change(urlInput, { target: { value: 'https://example.com' } });

        // Click Create
        const createButton = screen.getByText('Create');
        fireEvent.click(createButton);

        await waitFor(() => {
            expect(screen.getByText('A plugin with this name already exists')).toBeInTheDocument();
        });
    });

    it('navigates back to source selection via Back button', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        selectCustomSource();

        // Should be on config step
        expect(screen.getByTestId('plugin-configuration-step')).toBeInTheDocument();

        // Click Back
        fireEvent.click(screen.getByTestId('plugin-back-button'));

        // Should be back on source selection
        expect(screen.getByTestId('source-card-custom')).toBeInTheDocument();
    });

    // ------------------------------------------------------------------
    // Dynamic titles and subtitles
    // ------------------------------------------------------------------

    it('shows source-specific title after selecting GitHub', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        fireEvent.click(screen.getByTestId('source-card-github'));

        expect(screen.getByText('Create GitHub MCP Plugin')).toBeInTheDocument();
    });

    it('shows source-specific title after selecting dbt', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        fireEvent.click(screen.getByTestId('source-card-dbt'));

        expect(screen.getByText('Create dbt Cloud MCP Plugin')).toBeInTheDocument();
    });

    it('shows source-specific title after selecting Snowflake', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        fireEvent.click(screen.getByTestId('source-card-snowflake'));

        expect(screen.getByText('Create Snowflake MCP Plugin')).toBeInTheDocument();
    });

    it('shows Create Custom MCP Plugin title after selecting Custom', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        selectCustomSource();

        expect(screen.getByText('Create Custom MCP Plugin')).toBeInTheDocument();
    });

    // ------------------------------------------------------------------
    // AI Instructions section
    // ------------------------------------------------------------------

    it('shows AI Instructions textarea after selecting a source', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        selectCustomSource();

        expect(screen.getByText('Instructions for the AI Assistant')).toBeInTheDocument();
    });

    // ------------------------------------------------------------------
    // Learn More link
    // ------------------------------------------------------------------

    it('shows Learn More link for GitHub source', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        fireEvent.click(screen.getByTestId('source-card-github'));

        expect(screen.getByText('Learn More ↗')).toBeInTheDocument();
    });

    it('does not show Learn More link on source selection step', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        expect(screen.queryByText('Learn More ↗')).not.toBeInTheDocument();
    });

    // ------------------------------------------------------------------
    // Plugin name placeholder
    // ------------------------------------------------------------------

    it('shows "Plugin name" placeholder in name input', () => {
        render(
            <TestWrapper mocks={[]}>
                <CreatePluginModal editingPlugin={null} onClose={mockOnClose} />
            </TestWrapper>,
        );

        selectCustomSource();

        const nameInput = screen.getByTestId('plugin-name-input').querySelector('input')!;
        expect(nameInput.getAttribute('placeholder')).toBe('Plugin name');
    });
});
