import React, { useState } from 'react';
import { Button, Form, Input, message, Modal, Typography, Switch } from 'antd';
import { useCreatePostMutation } from '../../graphql/post.generated';
import { MediaType, PostType, PostContentType } from '../../types.generated';
import { useEnterKeyListener } from '../shared/useEnterKeyListener';

// const SuggestedNamesGroup = styled.div`
//     margin-top: 12px;
// `;
//
// const ClickableTag = styled(Tag)`
//     :hover {
//         cursor: pointer;
//     }
// `;

type Props = {
    onClose: () => void;
    onCreate: (
        contentType: string,
        title: string,
        description: string | undefined,
        link: string | undefined,
        location: string | undefined,
    ) => void;
};

const TITLE_FIELD_NAME = 'title';
const DESCRIPTION_FIELD_NAME = 'description';
const LINK_FIELD_NAME = 'link';
const LOCATION_FIELD_NAME = 'location';
const SWITCH_FIELD_NAME = 'switch';

export default function CreatePostModal({ onClose, onCreate }: Props) {
    const [isLinkPost, setisLinkPost] = useState<boolean>(false);
    const [createPostMutation] = useCreatePostMutation();
    const [createButtonEnabled, setCreateButtonEnabled] = useState(false);
    const [form] = Form.useForm();
    const onCreatePost = () => {
        const contentTypeValue = form.getFieldValue(SWITCH_FIELD_NAME) ? PostContentType.Link : PostContentType.Text;
        const mediaValue =
            form.getFieldValue(SWITCH_FIELD_NAME) && form.getFieldValue(LOCATION_FIELD_NAME)
                ? {
                      type: MediaType.Image,
                      location: form.getFieldValue(LOCATION_FIELD_NAME) ?? null,
                  }
                : null;
        createPostMutation({
            variables: {
                input: {
                    postType: PostType.HomePageAnnouncement,
                    content: {
                        contentType: contentTypeValue,
                        title: form.getFieldValue(TITLE_FIELD_NAME),
                        description: form.getFieldValue(DESCRIPTION_FIELD_NAME) ?? null,
                        link: form.getFieldValue(LINK_FIELD_NAME) ?? null,
                        media: mediaValue,
                    },
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    // analytics.event({
                    //     type: EventType.CreatePostEvent,
                    // });
                    message.success({
                        content: `Created Post!`,
                        duration: 3,
                    });
                    onCreate(
                        form.getFieldValue(SWITCH_FIELD_NAME) ? PostContentType.Text : PostContentType.Link,
                        form.getFieldValue(TITLE_FIELD_NAME),
                        form.getFieldValue(DESCRIPTION_FIELD_NAME),
                        form.getFieldValue(LINK_FIELD_NAME),
                        form.getFieldValue(LOCATION_FIELD_NAME),
                    );
                    form.resetFields();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({ content: `Failed to create Post!: \n ${e.message || ''}`, duration: 3 });
            });
        onClose();
    };

    // Handle the Enter press
    useEnterKeyListener({
        querySelectorToExecuteClick: '#createPostButton',
    });

    return (
        <Modal
            title="Create new Post"
            open
            onCancel={onClose}
            footer={
                <>
                    <Button onClick={onClose} type="text">
                        Cancel
                    </Button>
                    <Button
                        id="createPostButton"
                        data-testid="create-post-button"
                        onClick={onCreatePost}
                        disabled={!createButtonEnabled}
                    >
                        Create
                    </Button>
                </>
            }
        >
            <Form
                form={form}
                initialValues={{}}
                layout="vertical"
                onFieldsChange={() => {
                    setCreateButtonEnabled(!form.getFieldsError().some((field) => field.errors.length > 0));
                }}
            >
                <Form.Item label={<Typography.Text strong>Title</Typography.Text>}>
                    <Typography.Paragraph>Give your new Post a title. </Typography.Paragraph>
                    <Form.Item name={TITLE_FIELD_NAME} rules={[{ required: true }]} hasFeedback>
                        <Input data-testid="create-post-title" placeholder="A title for your Post" />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>
                        An optional description for your new Post. You can change this later.
                    </Typography.Paragraph>
                    <Form.Item name={DESCRIPTION_FIELD_NAME} rules={[{ min: 0, max: 500 }]} hasFeedback>
                        <Input.TextArea placeholder="A description for your Post" />
                    </Form.Item>
                </Form.Item>
                <Form.Item
                    name={SWITCH_FIELD_NAME}
                    label={<Typography.Text strong>Create link type post?</Typography.Text>}
                    valuePropName="checked"
                >
                    <Switch
                        defaultChecked={false}
                        onChange={(checked: boolean) => {
                            setisLinkPost(() => checked);
                        }}
                    />
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Link to content</Typography.Text>}>
                    <Typography.Paragraph>Where should users be redirected?</Typography.Paragraph>
                    <Form.Item name={LINK_FIELD_NAME} rules={[{ type: 'url', warningOnly: true }]} hasFeedback>
                        <Input
                            data-testid="create-post-link"
                            placeholder="A link for your Post"
                            disabled={!isLinkPost}
                        />
                    </Form.Item>
                </Form.Item>
                <Form.Item label={<Typography.Text strong>Media url</Typography.Text>}>
                    <Typography.Paragraph>Give your new Post a logo media. </Typography.Paragraph>
                    <Form.Item name={LOCATION_FIELD_NAME} rules={[{ type: 'url', warningOnly: true }]} hasFeedback>
                        <Input
                            data-testid="create-post-media-location"
                            placeholder="A media url location for your Post"
                            disabled={!isLinkPost}
                        />
                    </Form.Item>
                </Form.Item>
            </Form>
        </Modal>
    );
}
