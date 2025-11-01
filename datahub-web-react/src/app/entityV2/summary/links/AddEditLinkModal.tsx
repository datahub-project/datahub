import { Button, Checkbox, Input, Modal, Text } from '@components';
import { Form, FormInstance } from 'antd';
import React from 'react';
import styled from 'styled-components';

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
    initialValues?: {
        url?: string;
        label?: string;
        showInAssetPreview?: boolean;
    };
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
    return (
        <Modal
            title={`${variant === 'create' ? 'Add Link' : 'Edit Link'}`}
            onCancel={onClose}
            footer={
                <FooterContainer>
                    <FooterCheckboxContainer>
                        <Checkbox isChecked={showInAssetPreview} setIsChecked={setShowInAssetPreview} size="sm" />
                        <FooterCheckboxLabel color="gray" onClick={() => setShowInAssetPreview(!showInAssetPreview)}>
                            Add to asset header
                        </FooterCheckboxLabel>
                    </FooterCheckboxContainer>
                    <FooterButtonsContainer>
                        <Button variant="outline" onClick={onClose}>
                            Cancel
                        </Button>
                        <Button data-testid="link-form-modal-submit-button" onClick={onSubmit}>
                            {`${variant === 'create' ? 'Add Link' : 'Edit Link'}`}
                        </Button>
                    </FooterButtonsContainer>
                </FooterContainer>
            }
            destroyOnClose
        >
            <Form form={form} initialValues={initialValues} autoComplete="off">
                <Form.Item
                    name="url"
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
                    <Input label="URL" placeholder="https://" inputTestId="url-input" isRequired />
                </Form.Item>
                <Form.Item
                    name="label"
                    rules={[
                        {
                            required: true,
                            message: 'A label is required.',
                        },
                    ]}
                >
                    <Input
                        label="Label"
                        placeholder="A short label for this link"
                        inputTestId="label-input"
                        isRequired
                    />
                </Form.Item>
            </Form>
        </Modal>
    );
}
