import { Button, Form, message, Modal, Select } from 'antd';
import React, { useEffect } from 'react';
import { useAddOwnerMutation } from '../../../../../../../graphql/mutations.generated';
import { EntityType, OwnerEntityType, OwnershipType } from '../../../../../../../types.generated';
import { capitalizeFirstLetter } from '../../../../../../shared/capitalizeFirstLetter';
import { useEntityData } from '../../../../EntityContext';
import { LdapFormItem } from './LdapFormItem';

type Props = {
    visible: boolean;
    onClose: () => void;
    refetch?: () => Promise<any>;
};

export const AddOwnerModal = ({ visible, onClose, refetch }: Props) => {
    const [form] = Form.useForm();
    const { urn } = useEntityData();

    const [addOwnerMutation] = useAddOwnerMutation();

    useEffect(() => {
        form.setFieldsValue({
            ldap: '',
            role: OwnershipType.Stakeholder,
            type: EntityType.CorpUser,
        });
    }, [form]);

    const onOk = async () => {
        const row = await form.validateFields();
        try {
            const ownerEntityType =
                row.type === EntityType.CorpGroup ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser;
            await addOwnerMutation({
                variables: {
                    input: {
                        ownerUrn: `urn:li:${row.type === EntityType.CorpGroup ? 'corpGroup' : 'corpuser'}:${row.ldap}`,
                        resourceUrn: urn,
                        ownerEntityType,
                    },
                },
            });
            message.success({ content: 'Owner Added', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) {
                message.error({ content: `Failed to add owner: \n ${e.message || ''}`, duration: 3 });
            }
        }
        refetch?.();
        onClose();
    };

    return (
        <Modal
            title="Add owner"
            visible={visible}
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button onClick={onOk}>Add</Button>
                </>
            }
        >
            <Form form={form} component={false}>
                <LdapFormItem form={form} />
                <Form.Item
                    name="type"
                    rules={[
                        {
                            required: true,
                            type: 'string',
                            message: `Please select a type!`,
                        },
                    ]}
                >
                    <Select placeholder="Select a type" defaultValue={EntityType.CorpUser}>
                        <Select.Option value={EntityType.CorpUser} key={EntityType.CorpUser}>
                            User
                        </Select.Option>
                        <Select.Option value={EntityType.CorpGroup} key={EntityType.CorpGroup}>
                            Group
                        </Select.Option>
                    </Select>
                </Form.Item>
                <Form.Item
                    name="role"
                    rules={[
                        {
                            required: true,
                            type: 'string',
                            message: `Please select a role!`,
                        },
                    ]}
                >
                    <Select placeholder="Select a role">
                        {Object.values(OwnershipType).map((value) => (
                            <Select.Option value={value} key={value}>
                                {capitalizeFirstLetter(value)}
                            </Select.Option>
                        ))}
                    </Select>
                </Form.Item>
            </Form>
        </Modal>
    );
};
