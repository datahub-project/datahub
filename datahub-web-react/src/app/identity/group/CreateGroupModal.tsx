import React, { useRef, useState } from 'react';
import { message, Button, Input, Modal, Typography, Form, Collapse } from 'antd';
import styled from 'styled-components';
import { useCreateGroupMutation } from '../../../graphql/group.generated';
import { useEnterKeyListener } from '../../shared/useEnterKeyListener';
import { validateCustomUrnId } from '../../shared/textUtil';
import analytics, { EventType } from '../../analytics';
import { CorpGroup, EntityType } from '../../../types.generated';
import { Editor as MarkdownEditor } from '../../entity/shared/tabs/Documentation/components/editor/Editor';
import { ANTD_GRAY } from '../../entity/shared/constants';

type Props = {
    onClose: () => void;
    onCreate: (group: CorpGroup) => void;
};

const StyledEditor = styled(MarkdownEditor)`
    border: 1px solid ${ANTD_GRAY[4]};
`;

export default function CreateGroupModal({ onClose, onCreate }: Props) {
    const [stagedName, setStagedName] = useState('');
    const [stagedDescription, setStagedDescription] = useState('');
    const [stagedId, setStagedId] = useState<string | undefined>(undefined);
    const [createGroupMutation] = useCreateGroupMutation();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(true);
    const [form] = Form.useForm();

    // Reference to the styled editor for handling focus
    const styledEditorRef = useRef<HTMLDivElement>(null);

    const onCreateGroup = () => {
        // Check if the Enter key was pressed inside the styled editor to prevent unintended form submission
        const isEditorNewlineKeypress =
            document.activeElement !== styledEditorRef.current &&
            !styledEditorRef.current?.contains(document.activeElement);
        if (isEditorNewlineKeypress) {
            createGroupMutation({
                variables: {
                    input: {
                        id: stagedId,
                        name: stagedName,
                        description: stagedDescription,
                    },
                },
            })
                .then(({ data, errors }) => {
                    if (!errors) {
                        analytics.event({
                            type: EventType.CreateGroupEvent,
                        });
                        message.success({
                            content: `Created group!`,
                            duration: 3,
                        });
                        // TODO: Get a full corp group back from create endpoint.
                        onCreate({
                            urn: data?.createGroup || '',
                            type: EntityType.CorpGroup,
                            name: stagedName,
                            info: {
                                description: stagedDescription,
                            },
                        });
                    }
                })
                .catch((e) => {
                    message.destroy();
                    message.error({ content: `Failed to create group!: \n ${e.message || ''}`, duration: 3 });
                })
                .finally(() => {
                    setStagedName('');
                    setStagedDescription('');
                });
            onClose();
        }
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createGroupButton',
    });

    function updateDescription(description: string) {
        setStagedDescription(description);
    }

    return (
        <Modal
            width={700}
            title="Create new group"
            open
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button id="createGroupButton" onClick={onCreateGroup} disabled={createButtonEnabled}>
                        Create
                    </Button>
                </>
            }
        >
            <Form
                form={form}
                initialValues={{}}
                layout="vertical"
                onFieldsChange={() =>
                    setCreateButtonEnabled(form.getFieldsError().some((field) => field.errors.length > 0))
                }
            >
                <Form.Item label={<Typography.Text strong>Name</Typography.Text>}>
                    <Typography.Paragraph>Give your new group a name.</Typography.Paragraph>
                    <Form.Item
                        name="name"
                        rules={[
                            {
                                required: true,
                                message: 'Enter a Group name.',
                            },
                            { whitespace: true },
                            { min: 1, max: 50 },
                        ]}
                        hasFeedback
                    >
                        <Input
                            placeholder="A name for your group"
                            value={stagedName}
                            onChange={(event) => setStagedName(event.target.value)}
                        />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>An optional description for your new group.</Typography.Paragraph>
                    <Form.Item name="description" rules={[{ whitespace: true }]} hasFeedback>
                        {/* Styled editor for the group description */}
                        <div ref={styledEditorRef}>
                            <StyledEditor doNotFocus content={stagedDescription} onChange={updateDescription} />
                        </div>
                    </Form.Item>
                </Form.Item>
                <Collapse ghost>
                    <Collapse.Panel header={<Typography.Text type="secondary">Advanced</Typography.Text>} key="1">
                        <Form.Item label={<Typography.Text strong>Group Id</Typography.Text>}>
                            <Typography.Paragraph>
                                By default, a random UUID will be generated to uniquely identify this group. If
                                you&apos;d like to provide a custom id instead to more easily keep track of this group,
                                you may provide it here. Be careful, you cannot easily change the group id after
                                creation.
                            </Typography.Paragraph>
                            <Form.Item
                                name="groupId"
                                rules={[
                                    () => ({
                                        validator(_, value) {
                                            if (value && validateCustomUrnId(value)) {
                                                return Promise.resolve();
                                            }
                                            return Promise.reject(new Error('Please enter correct Group name'));
                                        },
                                    }),
                                ]}
                            >
                                <Input
                                    placeholder="product_engineering"
                                    value={stagedId || ''}
                                    onChange={(event) => setStagedId(event.target.value)}
                                />
                            </Form.Item>
                        </Form.Item>
                    </Collapse.Panel>
                </Collapse>
            </Form>
        </Modal>
    );
}
