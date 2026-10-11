import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { OperationType, isAddOperation, useBatchTagTermMutation } from '@app/shared/tags/useBatchTagTermMutation';
import { useEntityPickerState } from '@app/shared/tags/useEntityPickerState';
import TagPill from '@app/sharedV2/tags/TagPill';
import TagSelect from '@app/sharedV2/tags/TagSelect';
import CreateNewTagModal from '@app/tags/CreateNewTagModal/CreateNewTagModal';
import { Modal } from '@src/alchemy-components';
import { SelectOption } from '@src/alchemy-components/components/Select/types';
import { getModalDomContainer } from '@utils/focus';

import { Entity, EntityType, ResourceRefInput } from '@types';

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
    const { runMutation, disableAction } = useBatchTagTermMutation();
    const [createTagName, setCreateTagName] = useState<string | null>(null);

    const { urns, setUrns, removeUrn } = useEntityPickerState({
        entityType: EntityType.Tag,
        defaultValues,
    });

    // Pre-resolved entities for the default selection (e.g. editing an existing advanced
    // search filter) — seeds TagSelect's cache so their chips render with proper labels.
    const defaultEntities = useMemo(
        () => defaultValues.map((value) => value.entity).filter((entity): entity is Entity => !!entity),
        [defaultValues],
    );

    const renderOption = useCallback(
        (option: TagOption) => <TagPill name={option.label} color={option.color} colorHash={option.value} />,
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

    const handleCreateTag = useCallback((tagName: string) => {
        setCreateTagName(tagName);
    }, []);

    const handleUpdate = useCallback(
        (next: string[]) => {
            setUrns(next);
        },
        [setUrns],
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
            <TagSelect
                selectedUrns={urns}
                onUpdate={handleUpdate}
                renderOption={renderOption}
                renderSelectedValue={renderSelectedValue}
                showSearch
                placeholder={t('tagSearchPlaceholder')}
                width="full"
                dataTestId="tag-term-modal-input"
                allowCreateTag={isAddOperation(operationType)}
                onCreateTag={handleCreateTag}
                existingUrns={existingUrns}
                defaultEntities={defaultEntities}
            />
        </Modal>
    );
}
