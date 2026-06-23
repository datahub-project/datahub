import { Button, Checkbox, Modal, Text } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { LinkFormWrapper } from '@app/entityV2/shared/components/links/LinkFormWrapper';
import { LinkFormData } from '@app/entityV2/shared/components/links/types';

const FooterContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
`;

const FooterButtonsContainer = styled.div`
    display: flex;
    gap: 16px;
    flex-direction: row;
    align-items: center;
`;

const FooterCheckboxContainer = styled.div`
    display: flex;
    gap: 4px;
    flex-direction: row;
    align-items: center;
`;

const FooterCheckboxLabel = styled(Text)`
    cursor: pointer;
`;

interface Props {
    variant: 'create' | 'update';
    form: FormInstance;
    initialValues?: Partial<LinkFormData>;
    onClose: () => void;
    onSubmit: () => void;
    showInAssetPreview: boolean;
    setShowInAssetPreview: React.Dispatch<React.SetStateAction<boolean>>;
}

export default function AddEditLinkModal({
    variant,
    form,
    initialValues,
    onClose,
    onSubmit,
    showInAssetPreview,
    setShowInAssetPreview,
}: Props) {
    const { t } = useTranslation('entity.shared.components');
    const { t: tc } = useTranslation('common.actions');
    return (
        <Modal
            data-testid="add-edit-link-modal"
            title={variant === 'create' ? t('links.addLink') : t('links.editLink')}
            onCancel={onClose}
            footer={
                <FooterContainer>
                    <FooterCheckboxContainer>
                        <Checkbox
                            isChecked={showInAssetPreview}
                            setIsChecked={setShowInAssetPreview}
                            size="sm"
                            dataTestId="show-in-asset-preview-checkbox"
                        />
                        <FooterCheckboxLabel color="gray" onClick={() => setShowInAssetPreview(!showInAssetPreview)}>
                            {t('links.addToAssetHeader')}
                        </FooterCheckboxLabel>
                    </FooterCheckboxContainer>
                    <FooterButtonsContainer>
                        <Button variant="outline" onClick={onClose}>
                            {tc('cancel')}
                        </Button>
                        <Button data-testid="link-form-modal-submit-button" onClick={onSubmit}>
                            {variant === 'create' ? t('links.addLink') : t('links.editLink')}
                        </Button>
                    </FooterButtonsContainer>
                </FooterContainer>
            }
            destroyOnClose
        >
            <Form form={form} initialValues={initialValues} autoComplete="off">
                <LinkFormWrapper initialValues={initialValues} />
            </Form>
        </Modal>
    );
}
