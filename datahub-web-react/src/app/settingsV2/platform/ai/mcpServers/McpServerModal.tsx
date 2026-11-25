import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { type ToolSelection, ToolSelector } from '@app/context/agents/ToolSelector';
import { ViewSelector } from '@app/context/agents/ViewSelector';
import { FormField, HelpText, Label, ModalScrollBody } from '@app/context/agents/sharedStyles';
import {
    collapseSelectionToGroups,
    useAvailableAgentTools,
    useAvailableToolGroups,
} from '@app/context/agents/toolDefinitions';
import { serverUpdateInput } from '@app/settingsV2/platform/ai/mcpServers/hooks/useMcpServerActions';
import {
    type McpServerRow,
    buildConnectionUrl,
    validateSlug,
} from '@app/settingsV2/platform/ai/mcpServers/utils/mcpServerDataUtils';
import { Button, Input, Modal, SimpleSelect, TextArea, toast } from '@src/alchemy-components';

import { useUpdateMcpServersMutation } from '@graphql/mcpServers.generated';
import { McpInstructionSize, type McpServerConfigInput } from '@types';

const PINNED_WRAP_PROPS = { style: { overflow: 'hidden' as const } };

const ConnectionUrlBox = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    padding: 8px 10px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
    background: ${(props) => props.theme.colors.bgSurface};
    font-family: monospace;
    font-size: 12px;
    color: ${(props) => props.theme.colors.text};
    word-break: break-all;
`;

interface McpServerModalProps {
    /** When set, the modal edits this server. When null, it creates a new scoped server. */
    editing: McpServerRow | null;
    /** Slugs already in use (for create-time duplicate validation). */
    existingSlugs: string[];
    /** Whether scoped servers are on: shows the View picker. Off, a stored View is kept as it is. */
    scopedServersEnabled: boolean;
    onClose: () => void;
}

function toolsToSelection(row: McpServerRow | null): ToolSelection[] {
    return (row?.config?.tools ?? []).map((t) => ({ refType: t.refType, toolId: t.toolId, enabled: t.enabled }));
}

export const McpServerModal: React.FC<McpServerModalProps> = ({
    editing,
    existingSlugs,
    scopedServersEnabled,
    onClose,
}) => {
    const { t } = useTranslation('settings.ai');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcl } = useTranslation('common.labels');

    const isEdit = editing !== null;
    const isDefault = editing?.isDefault ?? false;

    const [slug, setSlug] = useState(editing?.slug ?? '');
    const [name, setName] = useState(editing?.name ?? '');
    const [description, setDescription] = useState(editing?.description ?? '');
    const [instructions, setInstructions] = useState(editing?.config?.customInstructions ?? '');
    const [instructionSize, setInstructionSize] = useState<McpInstructionSize>(
        editing?.config?.instructionSize ?? McpInstructionSize.Large,
    );
    const [tools, setTools] = useState<ToolSelection[]>(toolsToSelection(editing));
    const [viewUrn, setViewUrn] = useState<string | undefined>(editing?.config?.viewUrn ?? undefined);
    const [saving, setSaving] = useState(false);

    const allTools = useAvailableAgentTools();
    const toolGroups = useAvailableToolGroups(allTools);

    const [updateMcpServers] = useUpdateMcpServersMutation();

    // Slug is immutable after creation, so duplicate-check only matters on create.
    const slugError = useMemo(() => {
        if (isEdit || !slug) return undefined;
        const result = validateSlug(slug, new Set(existingSlugs));
        if (result.valid) return undefined;
        if (result.reason === 'format') return t('platform.ai.mcpServers.slugFormatError');
        if (result.reason === 'reserved') return t('platform.ai.mcpServers.slugReservedError');
        return t('platform.ai.mcpServers.slugDuplicateError');
    }, [slug, existingSlugs, isEdit, t]);

    const connectionUrl = buildConnectionUrl(isDefault ? undefined : slug || undefined);
    const canSave = !!name.trim() && (isDefault || (!!slug && !slugError));

    const handleCopyUrl = useCallback(() => {
        navigator.clipboard?.writeText(connectionUrl);
        toast.success(t('platform.ai.mcpServers.urlCopied'));
    }, [connectionUrl, t]);

    const handleSave = useCallback(async () => {
        if (!canSave) return;

        // Collapse fully-on/fully-off groups into a single GROUP ref so we persist the
        // group (and its future members), not a snapshot of each tool. Without a catalog
        // (still loading, or unavailable) the selection is saved exactly as it was.
        const normalizedTools = allTools.length > 0 ? collapseSelectionToGroups(tools, toolGroups, allTools) : tools;

        const serverInput: McpServerConfigInput = {
            slug: isDefault ? undefined : slug,
            name: name.trim(),
            description: description.trim() || undefined,
            // Preserve the current on/off state; the list view owns the toggle.
            enabled: editing?.enabled ?? true,
            customInstructions: instructions.trim() || undefined,
            instructionSize,
            tools: normalizedTools.length > 0 ? normalizedTools : undefined,
            viewUrn: viewUrn || undefined,
        };

        setSaving(true);
        try {
            await updateMcpServers({ variables: { input: serverUpdateInput(isDefault, serverInput) } });
            toast.success(
                isEdit ? t('platform.ai.mcpServers.updateSuccess') : t('platform.ai.mcpServers.createSuccess'),
            );
            onClose();
        } catch (e) {
            toast.error(t('platform.ai.mcpServers.saveError'));
            console.error('Failed to save MCP server:', e);
        } finally {
            setSaving(false);
        }
    }, [
        canSave,
        name,
        isDefault,
        slug,
        description,
        instructions,
        instructionSize,
        tools,
        allTools,
        toolGroups,
        viewUrn,
        editing,
        isEdit,
        updateMcpServers,
        onClose,
        t,
    ]);

    let title = t('platform.ai.mcpServers.createTitle');
    // Without scoped servers the default server is the only one, so it isn't called "Default".
    if (isDefault && scopedServersEnabled) title = t('platform.ai.mcpServers.editDefaultTitle');
    else if (isEdit) title = t('platform.ai.mcpServers.editTitle');

    return (
        <Modal
            open
            title={title}
            onCancel={onClose}
            width={720}
            wrapProps={PINNED_WRAP_PROPS}
            buttons={[
                { text: tc('cancel'), variant: 'outline', onClick: onClose },
                {
                    text: isEdit ? tc('save') : tc('create'),
                    variant: 'filled',
                    onClick: handleSave,
                    disabled: !canSave || saving,
                    isLoading: saving,
                    buttonDataTestId: 'mcp-server-save',
                },
            ]}
        >
            <ModalScrollBody>
                {!isDefault && (
                    <FormField>
                        <Input
                            label={t('platform.ai.mcpServers.slugLabel')}
                            placeholder={t('platform.ai.mcpServers.slugPlaceholder')}
                            value={slug}
                            setValue={setSlug}
                            isDisabled={isEdit}
                        />
                        <HelpText data-testid={slugError ? 'mcp-slug-error' : undefined}>
                            {slugError ?? t('platform.ai.mcpServers.slugHelp')}
                        </HelpText>
                    </FormField>
                )}

                <FormField>
                    <Input
                        label={tcl('name')}
                        placeholder={t('platform.ai.mcpServers.namePlaceholder')}
                        value={name}
                        setValue={setName}
                    />
                </FormField>

                <FormField>
                    <TextArea
                        label={tcl('description')}
                        placeholder={t('platform.ai.mcpServers.descriptionPlaceholder')}
                        value={description}
                        onChange={(e) => setDescription(e.target.value)}
                        rows={2}
                    />
                </FormField>

                <FormField>
                    <Label>{t('platform.ai.mcpServers.baseInstructionsLabel')}</Label>
                    <SimpleSelect
                        options={[
                            {
                                value: McpInstructionSize.Large,
                                label: t('platform.ai.mcpServers.instructionSizeLarge'),
                            },
                            {
                                value: McpInstructionSize.Small,
                                label: t('platform.ai.mcpServers.instructionSizeSmall'),
                            },
                            { value: McpInstructionSize.None, label: t('platform.ai.mcpServers.instructionSizeNone') },
                        ]}
                        values={[instructionSize]}
                        onUpdate={(values) =>
                            setInstructionSize((values[0] as McpInstructionSize) ?? McpInstructionSize.Large)
                        }
                        width="full"
                        position="start"
                        showClear={false}
                    />
                    <HelpText>{t('platform.ai.mcpServers.baseInstructionsHelp')}</HelpText>
                </FormField>

                <FormField>
                    <Label>{t('platform.ai.mcpServers.instructionsLabel')}</Label>
                    <TextArea
                        placeholder={t('platform.ai.mcpServers.instructionsPlaceholder')}
                        value={instructions}
                        onChange={(e) => setInstructions(e.target.value)}
                        rows={4}
                    />
                </FormField>

                <FormField>
                    <ToolSelector
                        value={tools}
                        onChange={setTools}
                        showAgents
                        showMcpServerOnlyGroups
                        toolsHelpText={t('platform.ai.mcpServers.toolsHelp')}
                    />
                </FormField>

                {scopedServersEnabled && (
                    <FormField>
                        <Label>{t('platform.ai.mcpServers.scopeLabel')}</Label>
                        <ViewSelector value={viewUrn} onChange={setViewUrn} />
                        <HelpText>{t('platform.ai.mcpServers.scopeHelp')}</HelpText>
                    </FormField>
                )}

                <FormField>
                    <Label>{t('platform.ai.mcpServers.connectionUrlLabel')}</Label>
                    <ConnectionUrlBox data-testid="mcp-connection-url">
                        <span>{connectionUrl}</span>
                        <Button
                            variant="text"
                            size="sm"
                            onClick={handleCopyUrl}
                            icon={{ icon: Copy }}
                            aria-label={t('platform.ai.mcpServers.copyUrl')}
                        />
                    </ConnectionUrlBox>
                    <HelpText>{t('platform.ai.mcpServers.connectionUrlHelp')}</HelpText>
                </FormField>
            </ModalScrollBody>
        </Modal>
    );
};
