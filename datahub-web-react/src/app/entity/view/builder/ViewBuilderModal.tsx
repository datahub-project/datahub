import { Modal, Typography } from 'antd';
import i18next from 'i18next';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ViewBuilderForm } from '@app/entity/view/builder/ViewBuilderForm';
import { ViewBuilderMode } from '@app/entity/view/builder/types';
import { DEFAULT_BUILDER_STATE, ViewBuilderState } from '@app/entity/view/types';
import ClickOutside from '@app/shared/ClickOutside';
import { Button } from '@src/alchemy-components';
import { getModalDomContainer } from '@utils/focus';

const modalWidth = 700;
const modalStyle = { top: 40 };
const modalBodyStyle = { paddingRight: 60, paddingLeft: 60, paddingBottom: 40 };

const TitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const SaveButtonContainer = styled.div`
    width: 100%;
    display: flex;
    justify-content: right;
    gap: 8px;
`;

const CancelButton = styled(Button)`
    margin-right: 12px;
`;

type Props = {
    mode: ViewBuilderMode;
    urn?: string;
    initialState?: ViewBuilderState;
    onSubmit: (input: ViewBuilderState) => void;
    onCancel?: () => void;
};

const getTitleText = (mode, urn) => {
    if (mode === ViewBuilderMode.PREVIEW) {
        return i18next.t('entity.views:builder.titlePreview');
    }
    return urn !== undefined
        ? i18next.t('entity.views:builder.titleEdit')
        : i18next.t('entity.views:builder.titleCreateLegacy');
};

export const ViewBuilderModal = ({ mode, urn, initialState, onSubmit, onCancel }: Props) => {
    const { t } = useTranslation('entity.views');
    const { t: tc } = useTranslation('common.actions');
    const [viewBuilderState, setViewBuilderState] = useState<ViewBuilderState>(initialState || DEFAULT_BUILDER_STATE);

    useEffect(() => {
        setViewBuilderState(initialState || DEFAULT_BUILDER_STATE);
    }, [initialState]);

    const confirmClose = () => {
        Modal.confirm({
            title: t('builder.exitTitle'),
            content: t('builder.exitTextLegacy'),
            onOk() {
                onCancel?.();
            },
            onCancel() {},
            okText: tc('yes'),
            maskClosable: true,
            closable: true,
        });
    };

    const canSave = viewBuilderState.name && viewBuilderState.viewType && viewBuilderState?.definition?.filter;
    const titleText = getTitleText(mode, urn);

    return (
        <ClickOutside onClickOutside={confirmClose} wrapperClassName="test-builder-modal">
            <Modal
                wrapClassName="view-builder-modal"
                footer={null}
                title={
                    <TitleContainer>
                        <Typography.Text>{titleText}</Typography.Text>
                    </TitleContainer>
                }
                style={modalStyle}
                bodyStyle={modalBodyStyle}
                open
                width={modalWidth}
                onCancel={onCancel}
                data-testid="view-modal"
                getContainer={getModalDomContainer}
            >
                <ViewBuilderForm urn={urn} mode={mode} state={viewBuilderState} updateState={setViewBuilderState} />
                <SaveButtonContainer>
                    <CancelButton variant="text" color="gray" data-testid="view-builder-cancel" onClick={onCancel}>
                        {tc('cancel')}
                    </CancelButton>
                    {mode === ViewBuilderMode.EDITOR && (
                        <Button
                            data-testid="view-builder-save"
                            disabled={!canSave}
                            onClick={() => onSubmit(viewBuilderState)}
                        >
                            {tc('save')}
                        </Button>
                    )}
                </SaveButtonContainer>
            </Modal>
        </ClickOutside>
    );
};
