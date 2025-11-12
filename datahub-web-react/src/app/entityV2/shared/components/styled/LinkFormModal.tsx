import { Form } from 'antd';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components/macro';

import { Button, Checkbox, Input, Modal, Text } from '@src/alchemy-components';

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

export interface FormData {
    url: string;
    label: string;
    showInAssetPreview: boolean;
}

interface Props {
    open: boolean;
    initialValues?: Partial<FormData>;
    variant: 'create' | 'update';
    onSubmit: (formData: FormData) => void;
    onCancel: () => void;
}

export const LinkFormModal = ({ open, initialValues, variant, onSubmit, onCancel }: Props) => {
    const [shouldBeShownInAssetPreview, setIsShowInAssetPreview] = useState<boolean>(
        !!initialValues?.showInAssetPreview,
    );
    const [form] = Form.useForm<FormData>();

    const onCancelHandler = useCallback(() => {
        onCancel();
    }, [onCancel]);

    // Reset form state to initial values when the form opened/closed
    useEffect(() => {
        form.resetFields();
        setIsShowInAssetPreview(!!initialValues?.showInAssetPreview);
    }, [open, form, initialValues?.showInAssetPreview]);

    // Sync shouldBeShownInAssetPreview with form field as this checkbox out of scope of form (in the modal's footer)
    useEffect(() => {
        const formValue = form.getFieldValue('showInAssetPreview');
        if (shouldBeShownInAssetPreview !== formValue) {
            form.setFieldValue('showInAssetPreview', shouldBeShownInAssetPreview);
        }
    }, [form, shouldBeShownInAssetPreview]);

    const title = useMemo(() => (variant === 'create' ? 'Add Link' : 'Update Link'), [variant]);
    const submitButtonText = useMemo(() => (variant === 'create' ? 'Create' : 'Update'), [variant]);

    return (
        <Modal
            title={title}
            open={open}
            destroyOnClose
            onCancel={onCancelHandler}
            footer={
                <FooterContainer>
                    <FooterCheckboxContainer>
                        <Checkbox
                            isChecked={shouldBeShownInAssetPreview}
                            setIsChecked={setIsShowInAssetPreview}
                            size="sm"
                            dataTestId="link-form-modal-show-in-asset-preview"
                        />
                        <FooterCheckboxLabel
                            color="gray"
                            onClick={() => setIsShowInAssetPreview(!shouldBeShownInAssetPreview)}
                        >
                            Add to asset header
                        </FooterCheckboxLabel>
                    </FooterCheckboxContainer>
                    <FooterButtonsContainer>
                        <Button variant="text" onClick={onCancelHandler}>
                            Cancel
                        </Button>
                        <Button data-testid="link-form-modal-submit-button" form="linkForm" key="submit">
                            {submitButtonText}
                        </Button>
                    </FooterButtonsContainer>
                </FooterContainer>
            }
        >
            <Form form={form} name="linkForm" onFinish={onSubmit} layout="vertical">
                <Form.Item
                    data-testid="link-form-modal-url"
                    name="url"
                    initialValue={initialValues?.url}
                    rules={[
                        {
                            required: true,
                            message: 'A URL is required.',
                        },
                        {
                            type: 'url',
                            message: 'This field must be a valid url.',
                        },
                    ]}
                >
                    <Input label="URL" placeholder="https://" autoFocus />
                </Form.Item>

                <Form.Item
                    data-testid="link-form-modal-label"
                    name="label"
                    initialValue={initialValues?.label}
                    rules={[
                        {
                            required: true,
                            message: 'A label is required.',
                        },
                    ]}
                >
                    <Input label="Label" placeholder="A short label for this link" />
                </Form.Item>

                <Form.Item
                    data-testid="link-modal-show-in-asset-preview"
                    name="showInAssetPreview"
                    valuePropName="checked"
                    initialValue={initialValues?.showInAssetPreview}
                    hidden
                >
                    <input type="checkbox" />
                </Form.Item>
            </Form>
        </Modal>
    );
};
