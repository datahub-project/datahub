import React, { useEffect, useState } from 'react';
import { Form, Input, Typography, FormInstance, Radio } from 'antd';
import styled from 'styled-components';
import { Editor } from '@src/app/entity/shared/tabs/Documentation/components/editor/Editor';
import { ANTD_GRAY } from '@src/app/entity/shared/constants';
import {
    DESCRIPTION_FIELD_NAME,
    LINK_FIELD_NAME,
    LOCATION_FIELD_NAME,
    TITLE_FIELD_NAME,
    TYPE_FIELD_NAME,
} from './constants';
import { PostContentType } from '../../../types.generated';
import { PostEntry } from './PostsListColumns';

const TopFormItem = styled(Form.Item)`
    margin-bottom: 24px;
`;

const SubFormItem = styled(Form.Item)`
    margin-bottom: 0;
`;

const StyledEditor = styled(Editor)`
    border: 1px solid ${ANTD_GRAY[4.5]};
`;

type Props = {
    setCreateButtonEnabled: (isEnabled: boolean) => void;
    form: FormInstance;
    contentType: PostContentType;
    editData?: PostEntry;
};

export default function CreatePostForm({ setCreateButtonEnabled, form, editData, contentType }: Props) {
    const [postType, setPostType] = useState<PostContentType>(PostContentType.Text);

    useEffect(() => {
        if (contentType) {
            setPostType(contentType);
        }
    }, [contentType]);

    return (
        <Form
            form={form}
            initialValues={{}}
            layout="vertical"
            onFieldsChange={() => {
                setCreateButtonEnabled(!form.getFieldsError().some((field) => field.errors.length > 0));
            }}
        >
            <TopFormItem name={TYPE_FIELD_NAME} label={<Typography.Text strong>Content Type</Typography.Text>}>
                <Radio.Group
                    onChange={(e) => setPostType(e.target.value)}
                    value={postType}
                    defaultValue={postType}
                    optionType="button"
                    buttonStyle="solid"
                >
                    <Radio value={PostContentType.Text}>Announcement</Radio>
                    <Radio value={PostContentType.Link}>Pinned Link</Radio>
                </Radio.Group>
            </TopFormItem>

            <TopFormItem label={<Typography.Text strong>Title</Typography.Text>}>
                <Typography.Paragraph>The title for your announcement or link.</Typography.Paragraph>
                <SubFormItem name={TITLE_FIELD_NAME} rules={[{ required: true }]} hasFeedback>
                    <Input data-testid="create-post-title" placeholder="Your title" />
                </SubFormItem>
            </TopFormItem>
            {postType === PostContentType.Text && (
                <TopFormItem label={<Typography.Text strong>Description</Typography.Text>}>
                    <Typography.Paragraph>The main content for your announcement.</Typography.Paragraph>
                    <SubFormItem name={DESCRIPTION_FIELD_NAME} rules={[{ min: 0, max: 500 }]} hasFeedback>
                        <StyledEditor
                            className="create-post-description"
                            doNotFocus
                            content={editData?.description || undefined}
                            onKeyDown={(e) => {
                                // Preventing the modal from closing when the Enter key is pressed
                                if (e.key === 'Enter') {
                                    e.preventDefault();
                                    e.stopPropagation();
                                }
                            }}
                        />
                    </SubFormItem>
                </TopFormItem>
            )}
            {postType === PostContentType.Link && (
                <>
                    <TopFormItem label={<Typography.Text strong>Link URL</Typography.Text>}>
                        <Typography.Paragraph>
                            Where users will be redirected when they click the link.
                        </Typography.Paragraph>
                        <SubFormItem name={LINK_FIELD_NAME} rules={[{ type: 'url', warningOnly: true }]} hasFeedback>
                            <Input data-testid="create-post-link" placeholder="Your link URL" />
                        </SubFormItem>
                    </TopFormItem>
                    <TopFormItem label={<Typography.Text strong>Image URL (Optional)</Typography.Text>}>
                        <Typography.Paragraph>
                            An optional URL to an image you want to display on your link.
                        </Typography.Paragraph>
                        <SubFormItem
                            name={LOCATION_FIELD_NAME}
                            rules={[{ type: 'url', warningOnly: true }]}
                            hasFeedback
                        >
                            <Input data-testid="create-post-media-location" placeholder="Your image URL" />
                        </SubFormItem>
                    </TopFormItem>
                    <SubFormItem label={<Typography.Text strong>Description</Typography.Text>}>
                        <Typography.Paragraph>A description for your link.</Typography.Paragraph>
                        <SubFormItem name={DESCRIPTION_FIELD_NAME} rules={[{ min: 0, max: 500 }]} hasFeedback>
                            <StyledEditor
                                doNotFocus
                                content={editData?.description || undefined}
                                className="create-post-description"
                                onKeyDown={(e) => {
                                    // Preventing the modal from closing when the Enter key is pressed
                                    if (e.key === 'Enter') {
                                        e.preventDefault();
                                        e.stopPropagation();
                                    }
                                }}
                            />
                        </SubFormItem>
                    </SubFormItem>
                </>
            )}
        </Form>
    );
}
