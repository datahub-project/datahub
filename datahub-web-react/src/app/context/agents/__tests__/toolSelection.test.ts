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

const tool = (toolId: string, label: string, groupId: string, optIn = false): ToolDefinition => ({
    toolId,
    label,
    description: '',
    groups: [groupId],
    optIn,
});
const SEARCH = tool('search', 'Search', 'read_metadata');
const TAGS = tool('add_tags', 'Add Tags', 'write_metadata');
const EVALS = tool('list_evals', 'List Evals', 'evals', true);
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
    it('keeps an opt-in group on when including every tool', () => {
        const next = setAllToolsSelection([group('read_metadata'), group('evals')], TOOLS, MATERIALIZED, false);
        expect(next).toEqual([...MATERIALIZED, group('evals')]);
    });

    it('excludes every tool but keeps agent refs', () => {
        const next = setAllToolsSelection([agent('urn:li:aiAgent:a')], TOOLS, MATERIALIZED, true);
        expect(next).toEqual([
            { refType: ToolRefType.Tool, toolId: 'search', enabled: false },
            { refType: ToolRefType.Tool, toolId: 'add_tags', enabled: false },
            agent('urn:li:aiAgent:a'),
        ]);
    });
});
