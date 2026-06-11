import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { FORBIDDEN_URN_CHARS_REGEX } from '@app/entity/shared/utils';
import { OperationType, isAddOperation, useBatchTagTermMutation } from '@app/shared/tags/useBatchTagTermMutation';
import { useEntityPickerState } from '@app/shared/tags/useEntityPickerState';
import TagPill from '@app/sharedV2/tags/TagPill';
import CreateNewTagModal from '@app/tags/CreateNewTagModal/CreateNewTagModal';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Modal, SimpleSelect } from '@src/alchemy-components';
import { SelectOption } from '@src/alchemy-components/components/Select/types';
import { getModalDomContainer } from '@utils/focus';

import { Entity, EntityType, ResourceRefInput, Tag } from '@types';

// Sentinel value for the synthetic "Create <name>" option appended to the dropdown when the
// typed query doesn't match any existing tag. Picking this option opens `CreateNewTagModal`
// instead of toggling selection — the value never actually lands in `urns`.
const CREATE_TAG_VALUE = '____reserved____.createTagValue';

const isValidTagName = (name: string) => name.length > 0 && !FORBIDDEN_URN_CHARS_REGEX.test(name);

type Props = {
    open: boolean;
    onCloseModal: () => void;
    resources: ResourceRefInput[];
    operationType?: OperationType;
    defaultValues?: { urn: string; entity?: Entity | null }[];
    /** Bypass the mutation entirely — used by `AdvancedFilterSelectValueModal` for filter selection. */
    onOkOverride?: (result: string[]) => void;
    /** URNs already applied to the resource(s); excluded from the dropdown so the user can only ADD new items. */
    existingUrns?: string[];
};

interface TagOption extends SelectOption {
    entity?: Entity;
    color?: string;
}

const OptionRow = styled.div`
    display: inline-flex;
    align-items: center;
    gap: 8px;
    min-width: 0;
`;

const CreateOptionLabel = styled.span`
    color: ${(props) => props.theme.colors.textBrand};
    font-weight: 500;
`;

const toOption = (entity: Entity, displayName: string): TagOption => ({
    value: entity.urn,
    label: (entity as Tag).name || displayName,
    entity,
    color: (entity as Tag).properties?.colorHex || undefined,
});

export default function AddTagsModal({
    open,
    onCloseModal,
    resources,
    operationType = OperationType.ADD,
    defaultValues = [],
    onOkOverride,
    existingUrns,
}: Props) {
    const { t } = useTranslation('shared.tags');
    const { t: tc } = useTranslation('common.actions');
    const entityRegistry = useEntityRegistry();
    const { runMutation, disableAction } = useBatchTagTermMutation();
    const [createTagName, setCreateTagName] = useState<string | null>(null);

    const { urns, setUrns, removeUrn, entityCache, searchText, handleSearch, currentEntities, isLoading } =
        useEntityPickerState({
            entityType: EntityType.Tag,
            defaultValues,
        });

    const excludeSet = useMemo(() => new Set(existingUrns || []), [existingUrns]);

    const dropdownOptions = useMemo<TagOption[]>(() => {
        const opts = currentEntities.map((e) => toOption(e, entityRegistry.getDisplayName(e.type, e)));
        const filtered = excludeSet.size === 0 ? opts : opts.filter((o) => !excludeSet.has(o.value));

        // Append a synthetic "Create <name>" option when the typed query doesn't match any existing
        // tag — mirrors the legacy `AddTagsTermsModal` behavior. Only offered on ADD operations and
        // when no items are already selected (the create flow assigns exactly one new tag).
        const trimmed = searchText.trim();
        const exactMatch = filtered.some(
            (o) => typeof o.label === 'string' && o.label.toLowerCase() === trimmed.toLowerCase(),
        );
        const showCreate =
            isAddOperation(operationType) &&
            trimmed.length > 0 &&
            isValidTagName(trimmed) &&
            !exactMatch &&
            urns.length === 0;
        if (showCreate) {
            filtered.push({ value: CREATE_TAG_VALUE, label: t('createOption', { inputValue: trimmed }) });
        }
        return filtered;
    }, [currentEntities, entityRegistry, excludeSet, operationType, searchText, urns.length, t]);

    const combinedOptions = useMemo<TagOption[]>(() => {
        const inDropdown = new Set(dropdownOptions.map((o) => o.value));
        const extras = urns
            .filter((urn) => !inDropdown.has(urn))
            .map<TagOption>((urn) => {
                const entity = entityCache[urn];
                if (entity) return toOption(entity, entityRegistry.getDisplayName(entity.type, entity));
                return { value: urn, label: urn };
            });
        return [...dropdownOptions, ...extras];
    }, [dropdownOptions, urns, entityCache, entityRegistry]);

    const renderOption = useCallback(
        (option: TagOption) => (
            <OptionRow data-testid={`tag-term-option-${option.label}`}>
                {option.value === CREATE_TAG_VALUE ? (
                    <CreateOptionLabel>{option.label}</CreateOptionLabel>
                ) : (
                    <TagPill name={option.label} color={option.color} colorHash={option.value} />
                )}
            </OptionRow>
        ),
        [],
    );

    const renderSelectedValue = useCallback(
        (option: TagOption) => (
            <TagPill
                key={option.value}
                name={option.label}
                color={option.color}
                colorHash={option.value}
                onRemove={() => removeUrn(option.value)}
                dataTestId={`selected-${option.label}`}
            />
        ),
        [removeUrn],
    );

    const handleUpdate = useCallback(
        (next: string[]) => {
            // Intercept the synthetic "Create" sentinel: open CreateNewTagModal instead of letting
            // it land in the selected URNs.
            if (next.includes(CREATE_TAG_VALUE)) {
                setCreateTagName(searchText.trim());
                return;
            }
            setUrns(next);
        },
        [searchText, setUrns],
    );

    const onOk = () => {
        if (onOkOverride) {
            onOkOverride(urns);
            return;
        }
        runMutation({
            urns,
            resources,
            type: EntityType.Tag,
            operationType,
            onDone: () => {
                onCloseModal();
                setUrns([]);
            },
        });
    };

    const isAdd = isAddOperation(operationType);
    const actionLabel = isAdd ? tc('add') : tc('remove');

    if (createTagName !== null) {
        return (
            <CreateNewTagModal
                open={open}
                initialTagName={createTagName}
                resources={resources}
                onClose={() => {
                    setCreateTagName(null);
                    onCloseModal();
                }}
            />
        );
    }

    return (
        <Modal
            title={isAdd ? t('modal.addTagsTitle') : t('modal.removeTagsTitle')}
            open={open}
            onCancel={onCloseModal}
            buttons={[
                { text: tc('cancel'), variant: 'text', onClick: onCloseModal },
                {
                    text: actionLabel,
                    id: 'addTagButton',
                    buttonDataTestId: 'add-tag-term-from-modal-btn',
                    variant: 'filled',
                    disabled: urns.length === 0 || disableAction,
                    onClick: onOk,
                },
            ]}
            getContainer={getModalDomContainer}
        >
            <SimpleSelect
                isMultiSelect
                showSearch
                showClear={false}
                onSearchChange={handleSearch}
                values={urns}
                onUpdate={handleUpdate}
                options={dropdownOptions}
                combinedSelectedAndSearchOptions={combinedOptions}
                renderCustomOptionText={renderOption}
                renderCustomSelectedValue={renderSelectedValue}
                selectLabelProps={{ variant: 'custom' }}
                filterResultsByQuery={false}
                isLoading={isLoading}
                placeholder={t('tagSearchPlaceholder')}
                width="full"
                dataTestId="tag-term-modal-input"
            />
        </Modal>
    );
}
