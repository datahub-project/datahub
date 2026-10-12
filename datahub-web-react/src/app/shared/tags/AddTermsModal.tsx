import React from 'react';
import { useTranslation } from 'react-i18next';

import { OperationType, isAddOperation, useBatchTagTermMutation } from '@app/shared/tags/useBatchTagTermMutation';
import { useEntityPickerState } from '@app/shared/tags/useEntityPickerState';
import GlossarySelect from '@app/sharedV2/glossary/GlossarySelect';
import { Modal } from '@src/alchemy-components';
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

/**
 * Modal that lets the user add (or remove) glossary terms on one or more resources.
 *
 * The tree-browse / search UI lives in `GlossarySelect`, shared with the policy form's glossary
 * picker. This modal owns only the selection state and the batch mutation.
 */
export default function AddTermsModal({
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

    const { urns, setUrns } = useEntityPickerState({ entityType: EntityType.GlossaryTerm, defaultValues });

    const onOk = () => {
        if (onOkOverride) {
            onOkOverride(urns);
            return;
        }
        runMutation({
            urns,
            resources,
            type: EntityType.GlossaryTerm,
            operationType,
            onDone: () => {
                onCloseModal();
                setUrns([]);
            },
        });
    };

    const isAdd = isAddOperation(operationType);
    const actionLabel = isAdd ? tc('add') : tc('remove');

    return (
        <Modal
            title={isAdd ? t('modal.addTermsTitle') : t('modal.removeTermsTitle')}
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
            <GlossarySelect
                selectedUrns={urns}
                onUpdate={setUrns}
                placeholder={t('termSearchPlaceholder')}
                width="full"
                showSearch
                existingUrns={existingUrns}
                dataTestId="tag-term-modal-input"
                defaultValues={defaultValues}
            />
        </Modal>
    );
}
