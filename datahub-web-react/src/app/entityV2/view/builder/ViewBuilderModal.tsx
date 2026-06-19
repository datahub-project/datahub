import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { ViewBuilderForm } from '@app/entityV2/view/builder/ViewBuilderForm';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { DEFAULT_BUILDER_STATE, ViewBuilderState } from '@app/entityV2/view/types';
import ClickOutside from '@app/shared/ClickOutside';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { Modal } from '@src/alchemy-components';
import { ModalButton } from '@src/alchemy-components/components/Modal/Modal';

type Props = {
    mode: ViewBuilderMode;
    urn?: string;
    initialState?: ViewBuilderState;
    onSubmit: (input: ViewBuilderState) => void;
    onCancel?: () => void;
};

const MODAL_WIDTH = '60%';
const MODAL_WRAP_CLASS = 'view-builder-modal';
const CLICK_OUTSIDE_CLASS = 'test-builder-modal';
const MODAL_WRAP_PROPS = { style: { overflow: 'hidden' } };
const MODAL_BODY_STYLE = { overflow: 'hidden', maxHeight: '75vh' };

export const ViewBuilderModal = ({ mode, urn, initialState, onSubmit, onCancel }: Props) => {
    const { t } = useTranslation('entity.views');
    const { t: tc } = useTranslation('common.actions');
    const [viewBuilderState, setViewBuilderState] = useState<ViewBuilderState>(initialState || DEFAULT_BUILDER_STATE);
    const [showConfirmationModal, setShowConfirmationModal] = useState(false);

    useEffect(() => {
        setViewBuilderState(initialState || DEFAULT_BUILDER_STATE);
    }, [initialState]);

    const hasFilters = (viewBuilderState?.definition?.filter?.filters?.length ?? 0) > 0;
    const canSave = viewBuilderState.name && viewBuilderState.viewType && hasFilters;

    const titleText = useMemo(() => {
        if (mode === ViewBuilderMode.PREVIEW) return t('builder.titlePreview');
        return urn !== undefined ? t('builder.titleEdit') : t('builder.titleCreate');
    }, [mode, urn, t]);

    const footerButtons: ModalButton[] = useMemo(() => {
        const buttons: ModalButton[] = [
            {
                text: tc('cancel'),
                variant: 'text',
                color: 'gray',
                onClick: () => onCancel?.(),
                buttonDataTestId: 'view-builder-cancel',
            },
        ];

        if (mode === ViewBuilderMode.EDITOR) {
            buttons.push({
                text: tc('save'),
                onClick: () => onSubmit(viewBuilderState),
                disabled: !canSave,
                buttonDataTestId: 'view-builder-save',
            });
        }

        return buttons;
    }, [mode, onCancel, onSubmit, viewBuilderState, canSave, tc]);

    return (
        <ClickOutside onClickOutside={() => setShowConfirmationModal(true)} wrapperClassName={CLICK_OUTSIDE_CLASS}>
            <Modal
                wrapClassName={MODAL_WRAP_CLASS}
                wrapProps={MODAL_WRAP_PROPS}
                bodyStyle={MODAL_BODY_STYLE}
                buttons={footerButtons}
                title={titleText}
                subtitle={t('builder.subtitle')}
                onCancel={() => onCancel?.()}
                data-testid="view-modal"
                width={MODAL_WIDTH}
            >
                <ViewBuilderForm urn={urn} mode={mode} state={viewBuilderState} updateState={setViewBuilderState} />
            </Modal>
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={() => {
                    setShowConfirmationModal(false);
                }}
                handleConfirm={() => {
                    setShowConfirmationModal(false);
                    onCancel?.();
                }}
                modalTitle={t('builder.exitTitle')}
                modalText={t('builder.exitText')}
                confirmButtonText={tc('yes')}
            />
        </ClickOutside>
    );
};
