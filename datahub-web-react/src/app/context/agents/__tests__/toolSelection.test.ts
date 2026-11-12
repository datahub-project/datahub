import { describe, expect, it } from 'vitest';

import {
    type ToolDefinition,
    type ToolSelection,
    isToolSelectionEnabled,
    materializeSelection,
    setAllToolsSelection,
    toolGroupOptions,
} from '@app/context/agents/toolDefinitions';

import { ToolRefType } from '@types';

const SEARCH: ToolDefinition = { toolId: 'search', label: 'Search', description: '', groups: ['read_metadata'] };
const TAGS: ToolDefinition = { toolId: 'add_tags', label: 'Add Tags', description: '', groups: ['write_metadata'] };
const EVALS: ToolDefinition = { toolId: 'list_evals', label: 'List Evals', description: '', groups: ['evals'] };
const TOOLS = [SEARCH, TAGS];
const GROUPS = toolGroupOptions(TOOLS);
const MATERIALIZED = materializeSelection(GROUPS, []);

const group = (toolId: string, enabled = true): ToolSelection => ({ refType: ToolRefType.Group, toolId, enabled });
const agent = (toolId: string): ToolSelection => ({ refType: ToolRefType.Agent, toolId, enabled: true });

describe('isToolSelectionEnabled', () => {
    it('treats an empty selection as every tool except opt-in ones', () => {
        expect(isToolSelectionEnabled([], SEARCH)).toBe(true);
        expect(isToolSelectionEnabled([], EVALS)).toBe(false);
        expect(isToolSelectionEnabled([group('evals')], EVALS)).toBe(true);
    });

    it('lets a per-tool ref override its group', () => {
        const value = [group('read_metadata'), { refType: ToolRefType.Tool, toolId: 'search', enabled: false }];
        expect(isToolSelectionEnabled(value, SEARCH)).toBe(false);
    });
});

describe('setAllToolsSelection', () => {
    it('excludes every tool but keeps agent refs', () => {
        const next = setAllToolsSelection([agent('urn:li:aiAgent:a')], TOOLS, MATERIALIZED, true);
        expect(next).toEqual([
            { refType: ToolRefType.Tool, toolId: 'search', enabled: false },
            { refType: ToolRefType.Tool, toolId: 'add_tags', enabled: false },
            agent('urn:li:aiAgent:a'),
        ]);
    });
});
