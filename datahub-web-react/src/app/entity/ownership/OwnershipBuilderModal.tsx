import React, { useEffect, useState } from 'react';
import { Button, Form, Input, Modal, Typography, message, notification } from 'antd';
import styled from 'styled-components/macro';
import { useCreateOwnershipTypeMutation, useUpdateOwnershipTypeMutation } from '../../../graphql/ownership.generated';
import { OwnershipTypeEntity } from '../../../types.generated';
import { OwnershipTypeBuilderState } from './table/types';

const NAME_INPUT_TEST_ID = 'ownership-type-name-input';
const DESCRIPTION_INPUT_TEST_ID = 'ownership-type-description-input';

const TitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const TitleText = styled(Typography.Text)`
    font-size: 16px;
    font-weight: 700;
`;

const FormItemContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const FormItemTitle = styled(Typography.Text)`
    margin-bottom: 8px;
    font-weight: 700;
`;

const StyledFormItem = styled(Form.Item)`
    margin-bottom: 8px;
`;

const SaveButtonContainer = styled.div`
    width: 100%;
    display: flex;
    justify-content: right;
`;

const CancelButton = styled(Button)`
    margin-right: 12px;
`;

type Props = {
    isOpen: boolean;
    onClose: () => void;
    refetch: () => void;
    ownershipType?: OwnershipTypeEntity;
};

export const OwnershipBuilderModal = ({ isOpen, onClose, refetch, ownershipType }: Props) => {
    // State
    const [ownershipTypeBuilderState, setOwnershipTypeBuilderState] = useState<OwnershipTypeBuilderState>({
        name: ownershipType?.info?.name || ownershipType?.urn || '',
        description: ownershipType?.info?.description || '',
    });
    const setName = (name: string) => {
        setOwnershipTypeBuilderState({ ...ownershipTypeBuilderState, name });
    };
    const setDescription = (description: string) => {
        setOwnershipTypeBuilderState({ ...ownershipTypeBuilderState, description });
    };
    const [form] = Form.useForm();
    form.setFieldsValue(ownershipTypeBuilderState);

    // Side effects
    useEffect(() => {
        if (ownershipType) {
            const ownershipTypeName = ownershipType?.info?.name || ownershipType?.urn;
            const ownershipTypeDescription = ownershipType?.info?.description || '';
            setOwnershipTypeBuilderState({
                name: ownershipTypeName,
                description: ownershipTypeDescription,
            });
        } else {
            setOwnershipTypeBuilderState({
                name: '',
                description: '',
            });
        }
    }, [ownershipType]);

    // Queries
    const [createOwnershipTypeMutation] = useCreateOwnershipTypeMutation();
    const [updateOwnershipTypeMutation] = useUpdateOwnershipTypeMutation();

    const onCreateOwnershipType = () => {
        if (ownershipTypeBuilderState.name) {
            createOwnershipTypeMutation({
                variables: {
                    input: {
                        name: ownershipTypeBuilderState.name,
                        description: ownershipTypeBuilderState.description,
                    },
                },
            })
                .then(() => {
                    setName('');
                    setDescription('');
                    onClose();
                    notification.success({
                        message: `Success`,
                        description: 'Successfully created ownership type.',
                        placement: 'bottomLeft',
                        duration: 3,
                    });
                    setTimeout(() => {
                        refetch();
                    }, 3000);
                })
                .catch((e: unknown) => {
                    message.destroy();
                    if (e instanceof Error) {
                        message.error({
                            content: `Failed to create ownership type`,
                            duration: 3,
                        });
                    }
                });
        }
    };

    const onUpdateOwnershipType = () => {
        if (ownershipType) {
            updateOwnershipTypeMutation({
                variables: {
                    urn: ownershipType?.urn || '',
                    input: {
                        name: ownershipTypeBuilderState.name,
                        description: ownershipTypeBuilderState.description,
                    },
                },
            })
                .then(() => {
                    setName('');
                    setDescription('');
                    onClose();
                    notification.success({
                        message: `Success`,
                        description: 'Successfully updated ownership type.',
                        placement: 'bottomLeft',
                        duration: 3,
                    });
                    setTimeout(() => {
                        refetch();
                    }, 3000);
                })
                .catch((e: unknown) => {
                    message.destroy();
                    if (e instanceof Error) {
                        message.error({
                            content: `Failed to update ownership type`,
                            duration: 3,
                        });
                    }
                });
        }
    };

    const onUpsert = ownershipType ? onUpdateOwnershipType : onCreateOwnershipType;
    const titleText = ownershipType ? 'Edit ownership type' : 'Add a new ownership type';
    return (
        <Modal
            open={isOpen}
            onCancel={onClose}
            title={
                <TitleContainer>
                    <TitleText>{titleText}</TitleText>
                </TitleContainer>
            }
            footer={null}
        >
            <Form form={form}>
                <FormItemContainer>
                    <FormItemTitle>Name</FormItemTitle>
                    <StyledFormItem
                        name="name"
                        rules={[
                            {
                                required: true,
                                message: 'Please input a name for the ownership type',
                            },
                            { whitespace: true },
                            { min: 1, max: 50 },
                        ]}
                    >
                        <Input
                            data-testid={NAME_INPUT_TEST_ID}
                            placeholder="Ownership type name"
                            onChange={(e) => {
                                setName(e.target.value);
                            }}
                        />
                    </StyledFormItem>
                </FormItemContainer>
                <FormItemContainer>
                    <FormItemTitle>Description</FormItemTitle>
                    <StyledFormItem name="description" rules={[{ whitespace: true }, { min: 1, max: 250 }]}>
                        <Input
                            data-testid={DESCRIPTION_INPUT_TEST_ID}
                            placeholder="Ownership type description"
                            onChange={(e) => {
                                setDescription(e.target.value);
                            }}
                        />
                    </StyledFormItem>
                </FormItemContainer>
            </Form>
            <SaveButtonContainer>
                <CancelButton data-testid="ownership-builder-cancel" onClick={onClose}>
                    Cancel
                </CancelButton>
                <Button
                    data-testid="ownership-builder-save"
                    type="primary"
                    disabled={!ownershipTypeBuilderState.name}
                    onClick={onUpsert}
                >
                    Save
                </Button>
            </SaveButtonContainer>
        </Modal>
    );
};
