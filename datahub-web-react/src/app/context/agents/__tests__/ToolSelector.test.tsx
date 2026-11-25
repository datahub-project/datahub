import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { type ToolSelection, ToolSelector } from '@app/context/agents/ToolSelector';
import type { McpToolCatalog, McpToolCatalogState } from '@app/context/agents/useMcpToolCatalog';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { ToolRefType } from '@types';

const BASE_TOOLS: McpToolCatalog['tools'] = [
    { toolId: 'search', description: 'Search the catalog', groups: ['read_metadata'] },
    { toolId: 'add_tags', description: 'Tag assets', category: 'mutation', groups: ['write_metadata'] },
];
const EVAL_TOOL = { toolId: 'list_evals', description: 'List evals', groups: ['evals'] };
const INGESTION_TOOL = { toolId: 'validate_recipe', description: 'Check a recipe', groups: ['ingestion'] };
const AGENT = { urn: 'urn:li:aiAgent:revenue-helper', name: 'Revenue Helper', description: 'Custom' };

const mockCatalogState = vi.fn<() => McpToolCatalogState>();
vi.mock('@app/context/agents/useMcpToolCatalog', () => ({
    useMcpToolCatalog: () => mockCatalogState(),
}));

// The hooks memoize on the catalog object, so each test hands out one stable object.
function setCatalog(catalog: McpToolCatalog | undefined, state: Partial<McpToolCatalogState> = {}) {
    const value = { catalog, loading: false, failed: false, retry: vi.fn(), ...state };
    mockCatalogState.mockReturnValue(value);
    return value;
}

function renderSelector(props: Partial<React.ComponentProps<typeof ToolSelector>> = {}) {
    const onChange = vi.fn<(tools: ToolSelection[]) => void>();
    render(
        <CustomThemeProvider>
            <ToolSelector value={[]} onChange={onChange} showAgents showMcpServerOnlyGroups {...props} />
        </CustomThemeProvider>,
    );
    return onChange;
}

describe('ToolSelector', () => {
    beforeEach(() => setCatalog({ tools: BASE_TOOLS, agents: [] }));

    it('lists catalog tools under their groups, with no Agents tab or opt-in groups when the catalog has none', () => {
        renderSelector();

        expect(screen.getByRole('checkbox', { name: 'Read Metadata' })).toBeInTheDocument();
        expect(screen.getByText('Search')).toBeInTheDocument();
        expect(screen.queryByTestId('tool-selector-tab-agents')).not.toBeInTheDocument();
        expect(screen.queryByRole('checkbox', { name: 'Evals' })).not.toBeInTheDocument();
    });

    it('offers an Agents tab when the catalog lists agents', () => {
        setCatalog({ tools: BASE_TOOLS, agents: [AGENT] });
        const onChange = renderSelector();

        fireEvent.click(screen.getByTestId('tool-selector-tab-agents'));
        fireEvent.click(screen.getByRole('checkbox', { name: 'Revenue Helper' }));

        // Turning an agent on for an "all tools" server keeps every tool on.
        expect(onChange).toHaveBeenCalledWith([
            { refType: ToolRefType.Group, toolId: 'read_metadata', enabled: true },
            { refType: ToolRefType.Group, toolId: 'write_metadata', enabled: true },
            { refType: ToolRefType.Agent, toolId: AGENT.urn, enabled: true },
        ]);
    });

    it('offers opt-in groups only when the catalog has them, and MCP-server-only groups only when asked', () => {
        setCatalog({ tools: [...BASE_TOOLS, EVAL_TOOL, INGESTION_TOOL] });
        renderSelector({ showMcpServerOnlyGroups: false });

        expect(screen.getByRole('checkbox', { name: 'Evals' })).toBeInTheDocument();
        expect(screen.queryByRole('checkbox', { name: 'Ingestion' })).not.toBeInTheDocument();
    });

    it('turns one tool off by keeping the rest of its group on', () => {
        const onChange = renderSelector();

        fireEvent.click(screen.getByRole('checkbox', { name: 'Search' }));

        expect(onChange).toHaveBeenCalledWith([
            { refType: ToolRefType.Group, toolId: 'read_metadata', enabled: true },
            { refType: ToolRefType.Group, toolId: 'write_metadata', enabled: true },
            { refType: ToolRefType.Tool, toolId: 'search', enabled: false },
        ]);
    });

    it('shows a retry when the catalog fails to load', () => {
        const state = setCatalog(undefined, { failed: true });
        renderSelector();

        expect(screen.getByText('Failed to load tools.')).toBeInTheDocument();
        fireEvent.click(screen.getByText('Retry'));
        expect(state.retry).toHaveBeenCalled();
    });
});
