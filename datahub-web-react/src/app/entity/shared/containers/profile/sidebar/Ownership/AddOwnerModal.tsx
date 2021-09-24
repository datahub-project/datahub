import { Button, Form, Modal, Select } from 'antd';
import React, { useEffect } from 'react';
import { EntityType, Maybe, Owner, OwnershipType, OwnershipUpdate } from '../../../../../../../types.generated';
import { capitalizeFirstLetter } from '../../../../../../shared/capitalizeFirstLetter';
import { LdapFormItem } from './LdapFormItem';

type Props = {
    visible: boolean;
    onClose: () => void;
    updateOwnership: (update: OwnershipUpdate) => void;
    owners: Maybe<Owner[]> | undefined;
};

export const AddOwnerModal = ({ visible, onClose, owners, updateOwnership }: Props) => {
    const [form] = Form.useForm();
    useEffect(() => {
        form.setFieldsValue({
            ldap: '',
            role: OwnershipType.Stakeholder,
            type: EntityType.CorpUser,
        });
    }, [form]);

    const onOk = async () => {
        if (updateOwnership) {
            const row = await form.validateFields();
            const updatedOwners =
                owners?.map((owner) => {
                    return {
                        owner: owner.owner.urn,
                        type: owner.type,
                    };
                }) || [];
            updatedOwners.push({
                owner: `urn:li:${row.type === EntityType.CorpGroup ? 'corpGroup' : 'corpuser'}:${row.ldap}`,
                type: row.role,
            });
            updateOwnership({ owners: updatedOwners });
        }
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
