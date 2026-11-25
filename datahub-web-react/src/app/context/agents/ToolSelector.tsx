import { Button, Switch } from '@components';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { FieldHeader, HelpText, Label } from '@app/context/agents/sharedStyles';
import {
    type AgentOption,
    MCP_SERVER_ONLY_GROUP_IDS,
    type ToolDefinition,
    type ToolGroupOption,
    type ToolSelection,
    isAgentSelected,
    isOptInGroupSelected,
    isOptInTool,
    isToolGroupSelected,
    isToolSelectionEnabled,
    materializeSelection,
    setAgentSelection,
    setAllToolsSelection,
    setGroupSelection,
    setOptInGroupSelection,
    setToolSelection,
    toolGroupOptions,
    toolIdToLabel,
    truncateDescription,
    useAvailableAgentToolsState,
    useAvailableAgentsState,
    useAvailableToolGroups,
} from '@app/context/agents/toolDefinitions';
import { Tabs } from '@app/sharedV2/tabs/Tabs';

export type { ToolSelection } from '@app/context/agents/toolDefinitions';

const ToolList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
    max-height: 320px;
    overflow-y: auto;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
    padding: 4px;
`;

const ToolRow = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 6px 10px;
    border-radius: 6px;

    &:hover {
        background: ${(props) => props.theme.colors.bgHover};
    }
`;

// A section per group, so its sticky header gives way to the next group's.
const GroupSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
`;

// top: -4px covers the list's padding; the opaque background hides tools scrolling underneath.
const GroupRow = styled(ToolRow)`
    position: sticky;
    top: -4px;
    z-index: 1;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
    margin-bottom: 2px;
    background: ${(props) => props.theme.colors.bg};

    &:hover {
        background: ${(props) => props.theme.colors.bg};
    }
`;

const ToolInfo = styled.div`
    display: flex;
    flex-direction: column;
    gap: 1px;
    min-width: 0;
    flex: 1;
    margin-right: 12px;
`;

const ToolLabel = styled.span`
    font-size: 13px;
    font-weight: 500;
    color: ${(props) => props.theme.colors.text};
`;

const ToolDescription = styled.span`
    font-size: 11px;
    color: ${(props) => props.theme.colors.textSecondary};
    overflow: hidden;
    text-overflow: ellipsis;
`;

const GroupLabel = styled.span`
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    color: ${(props) => props.theme.colors.textTertiary};
`;

const StatusMessage = styled.div<{ $error?: boolean }>`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 12px 10px;
    font-size: 12px;
    color: ${(props) => (props.$error ? props.theme.colors.textError : props.theme.colors.textSecondary)};
`;

const SectionNote = styled.div`
    padding: 6px 10px 10px;
    font-size: 11px;
    line-height: 1.4;
    color: ${(props) => props.theme.colors.textSecondary};
`;

enum SelectorTab {
    Tools = 'tools',
    Agents = 'agents',
}

interface ToolSelectorProps {
    value: ToolSelection[];
    onChange: (tools: ToolSelection[]) => void;
    // When true, show an "Agents" tab listing the agents the tool catalog offers as
    // tools. The tab only appears when the catalog lists agents.
    showAgents?: boolean;
    // Agent URN to omit from the Agents list, so an agent can't select itself.
    excludeAgentUrn?: string;
    // Help text shown under the tool list only (not under the Agents tab).
    toolsHelpText?: string;
    // When true, also offer the opt-in groups only MCP servers attach (MCP_SERVER_ONLY_GROUP_IDS).
    showMcpServerOnlyGroups?: boolean;
}

export const ToolSelector: React.FC<ToolSelectorProps> = ({
    value,
    onChange,
    showAgents = false,
    excludeAgentUrn,
    toolsHelpText,
    showMcpServerOnlyGroups = false,
}) => {
    const { t } = useTranslation('settings.ai');
    const {
        items: fetchedTools,
        loading: toolsLoading,
        failed: toolsFailed,
        retry: retryTools,
    } = useAvailableAgentToolsState();
    const { items: allAgents } = useAvailableAgentsState();

    // Opt-in groups are held out of the main list, whose "all tools" selection
    // means the default server's tools and never includes them. They get their own
    // section below, and only appear when the catalog has tools in them.
    const allTools = useMemo(() => fetchedTools.filter((tool) => !isOptInTool(tool)), [fetchedTools]);
    const optInGroups = useMemo(
        () =>
            toolGroupOptions(fetchedTools.filter(isOptInTool)).filter(
                (group) => showMcpServerOnlyGroups || !MCP_SERVER_ONLY_GROUP_IDS.has(group.id),
            ),
        [fetchedTools, showMcpServerOnlyGroups],
    );
    const groups = useAvailableToolGroups(allTools);
    const agents = allAgents.filter((a) => a.urn !== excludeAgentUrn);
    // The Agents tab appears only when the catalog lists agents to expose.
    const agentsSectionEnabled = showAgents && agents.length > 0;
    const [activeTab, setActiveTab] = useState<SelectorTab>(SelectorTab.Tools);

    const toolsById = useMemo(() => new Map(allTools.map((tool) => [tool.toolId, tool])), [allTools]);
    const groupedToolIds = useMemo(() => new Set(groups.flatMap((g) => g.toolIds)), [groups]);
    const ungroupedTools = useMemo(
        () => allTools.filter((tool) => !groupedToolIds.has(tool.toolId)),
        [allTools, groupedToolIds],
    );

    const materialized = useMemo(() => materializeSelection(groups, ungroupedTools), [groups, ungroupedTools]);

    const isToolEnabled = (tool: ToolDefinition) => isToolSelectionEnabled(value, tool);
    const allEnabled = value.length === 0 || allTools.every((tool) => isToolEnabled(tool));

    const handleToggleAll = () => onChange(setAllToolsSelection(value, allTools, materialized, allEnabled));
    const handleToggleGroup = (groupId: string, checked: boolean) =>
        onChange(setGroupSelection(value, groupId, checked, materialized));
    const handleToggleTool = (toolId: string, checked: boolean) =>
        onChange(setToolSelection(value, toolId, checked, materialized));
    const handleToggleAgent = (agentUrn: string, checked: boolean) =>
        onChange(setAgentSelection(value, agentUrn, checked, materialized));
    const handleToggleOptInGroup = (groupId: string, checked: boolean) =>
        onChange(setOptInGroupSelection(value, groupId, checked, materialized));

    const renderSwitch = (label: string, isChecked: boolean, onToggle: (checked: boolean) => void) => (
        // The switch's own label is empty, so name it directly for assistive tech.
        <Switch
            label=""
            labelStyle={{ display: 'none' }}
            aria-labelledby={undefined}
            aria-label={label}
            isChecked={isChecked}
            onChange={(e) => onToggle(e.target.checked)}
        />
    );

    const renderTool = (tool: ToolDefinition) => (
        <ToolRow key={tool.toolId}>
            <ToolInfo>
                <ToolLabel>{tool.label}</ToolLabel>
                <ToolDescription>{truncateDescription(tool.description)}</ToolDescription>
            </ToolInfo>
            {renderSwitch(tool.label, isToolEnabled(tool), (checked) => handleToggleTool(tool.toolId, checked))}
        </ToolRow>
    );

    const renderGroup = (group: ToolGroupOption) => (
        <GroupSection key={group.id}>
            <GroupRow>
                <GroupLabel>{group.label}</GroupLabel>
                {renderSwitch(group.label, isToolGroupSelected(value, group.id), (checked) =>
                    handleToggleGroup(group.id, checked),
                )}
            </GroupRow>
            {group.toolIds.map((id) => toolsById.get(id)).flatMap((tool) => (tool ? [renderTool(tool)] : []))}
        </GroupSection>
    );

    const renderOptInGroup = (group: ToolGroupOption) => (
        <GroupSection key={`__optin_${group.id}`}>
            <GroupRow>
                <GroupLabel>{group.label}</GroupLabel>
                {renderSwitch(group.label, isOptInGroupSelected(value, group.id), (checked) =>
                    handleToggleOptInGroup(group.id, checked),
                )}
            </GroupRow>
            <SectionNote>
                {t('platform.ai.toolSelector.optInGroupNote', { tools: group.toolIds.map(toolIdToLabel).join(', ') })}
            </SectionNote>
        </GroupSection>
    );

    const renderAgent = (agent: AgentOption) => (
        <ToolRow key={agent.urn}>
            <ToolInfo>
                <ToolLabel>{agent.name}</ToolLabel>
                <ToolDescription>{truncateDescription(agent.description)}</ToolDescription>
            </ToolInfo>
            {renderSwitch(agent.name, isAgentSelected(value, agent.urn), (checked) =>
                handleToggleAgent(agent.urn, checked),
            )}
        </ToolRow>
    );

    // Bulk toggles are meaningless without a catalog to toggle.
    const toggleAllButton =
        allTools.length > 0 ? (
            <Button variant="link" size="sm" onClick={handleToggleAll}>
                {allEnabled ? t('platform.ai.toolSelector.excludeAll') : t('platform.ai.toolSelector.includeAll')}
            </Button>
        ) : null;
    const toolsHelp = toolsHelpText ? <HelpText>{toolsHelpText}</HelpText> : null;

    const toolList = (
        <ToolList data-testid="tool-selector-list">
            {toolsFailed && (
                <StatusMessage $error>
                    {t('platform.ai.toolSelector.loadFailed')}
                    <Button variant="link" size="sm" onClick={retryTools}>
                        {t('platform.ai.toolSelector.retry')}
                    </Button>
                </StatusMessage>
            )}
            {toolsLoading && <StatusMessage>{t('platform.ai.toolSelector.loading')}</StatusMessage>}
            {groups.map(renderGroup)}
            {optInGroups.map(renderOptInGroup)}
            {ungroupedTools.length > 0 && (
                <GroupSection key="__other__">
                    <GroupRow>
                        <GroupLabel>{t('platform.ai.toolSelector.otherTools')}</GroupLabel>
                    </GroupRow>
                    {ungroupedTools.map(renderTool)}
                </GroupSection>
            )}
        </ToolList>
    );

    if (!agentsSectionEnabled) {
        return (
            <>
                <FieldHeader>
                    <Label>{t('platform.ai.toolSelector.tools')}</Label>
                    {toggleAllButton}
                </FieldHeader>
                {toolList}
                {toolsHelp}
            </>
        );
    }

    const enabledToolCount =
        allTools.filter(isToolEnabled).length +
        optInGroups.filter((g) => isOptInGroupSelected(value, g.id)).reduce((n, g) => n + g.toolIds.length, 0);
    const enabledAgentCount = agents.filter((agent) => isAgentSelected(value, agent.urn)).length;

    return (
        <Tabs
            selectedTab={activeTab}
            onChange={(key) => setActiveTab(key as SelectorTab)}
            destroyInactiveTabPane
            styleOptions={{ containerHeight: 'auto' }}
            // The tab already names the list, so only the bulk toggle sits beside it.
            tabBarExtraContent={activeTab === SelectorTab.Tools ? toggleAllButton : undefined}
            tabs={[
                {
                    key: SelectorTab.Tools,
                    name: t('platform.ai.toolSelector.tools'),
                    count: toolsLoading ? undefined : enabledToolCount,
                    dataTestId: 'tool-selector-tab-tools',
                    component: (
                        <>
                            {toolList}
                            {toolsHelp}
                        </>
                    ),
                },
                {
                    key: SelectorTab.Agents,
                    name: t('platform.ai.toolSelector.agents'),
                    count: enabledAgentCount,
                    dataTestId: 'tool-selector-tab-agents',
                    component: (
                        <ToolList>
                            <SectionNote>{t('platform.ai.toolSelector.agentNote')}</SectionNote>
                            {agents.map(renderAgent)}
                        </ToolList>
                    ),
                },
            ]}
        />
    );
};
