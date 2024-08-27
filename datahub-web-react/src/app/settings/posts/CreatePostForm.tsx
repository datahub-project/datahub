import React, { useEffect, useState } from 'react';
import { Form, Input, Typography, FormInstance, Radio } from 'antd';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import {
    DESCRIPTION_FIELD_NAME,
    LINK_FIELD_NAME,
    LOCATION_FIELD_NAME,
    TITLE_FIELD_NAME,
    TYPE_FIELD_NAME,
} from './constants';
import { PostContentType } from '../../../types.generated';

const TopFormItem = styled(Form.Item)`
    margin-bottom: 24px;
`;

const SubFormItem = styled(Form.Item)`
    margin-bottom: 0;
`;

type Props = {
    setCreateButtonEnabled: (isEnabled: boolean) => void;
    form: FormInstance;
    contentType: PostContentType;
};

export default function CreatePostForm({ setCreateButtonEnabled, form, contentType }: Props) {
    const [postType, setPostType] = useState<PostContentType>(PostContentType.Text);
    const { t } = useTranslation();
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
            <TopFormItem name={TYPE_FIELD_NAME} label={<Typography.Text strong>{t('post.postType')}</Typography.Text>}>
                <Radio.Group
                    onChange={(e) => setPostType(e.target.value)}
                    value={postType}
                    defaultValue={postType}
                    optionType="button"
                    buttonStyle="solid"
                >
                    <Radio value={PostContentType.Text}>{t('common.announcement')}</Radio>
                    <Radio value={PostContentType.Link}>{t('common.link')}</Radio>
                </Radio.Group>
            </TopFormItem>

            <TopFormItem label={<Typography.Text strong>{t('common.title')}</Typography.Text>}>
                <Typography.Paragraph>{t('post.titleDescription')}</Typography.Paragraph>
                <SubFormItem name={TITLE_FIELD_NAME} rules={[{ required: true }]} hasFeedback>
                    <Input data-testid="create-post-title" placeholder={t('post.titlePlaceholder')} />
                </SubFormItem>
            </TopFormItem>
            {postType === PostContentType.Text && (
                <TopFormItem label={<Typography.Text strong>{t('common.description')}</Typography.Text>}>
                    <Typography.Paragraph>{t('post.descriptionDescription')}</Typography.Paragraph>
                    <SubFormItem name={DESCRIPTION_FIELD_NAME} rules={[{ min: 0, max: 500 }]} hasFeedback>
                        <Input.TextArea placeholder={t('post.titleDescription')} />
                    </SubFormItem>
                </TopFormItem>
            )}
            {postType === PostContentType.Link && (
                <>
                    <TopFormItem label={<Typography.Text strong>{t('post.linkUrl')}</Typography.Text>}>
                        <Typography.Paragraph>{t('post.linkUrlDescription')}</Typography.Paragraph>
                        <SubFormItem name={LINK_FIELD_NAME} rules={[{ type: 'url', warningOnly: true }]} hasFeedback>
                            <Input data-testid="create-post-link" placeholder={t('post.linkUrlPlaceholder')} />
                        </SubFormItem>
                    </TopFormItem>
                    <SubFormItem label={<Typography.Text strong>{t('post.imageUrl')}</Typography.Text>}>
                        <Typography.Paragraph>{t('post.imageUrlDescription')}</Typography.Paragraph>
                        <SubFormItem
                            name={LOCATION_FIELD_NAME}
                            rules={[{ type: 'url', warningOnly: true }]}
                            hasFeedback
                        >
                            <Input
                                data-testid="create-post-media-location"
                                placeholder={t('post.imageUrlPlaceholder')}
                            />
                        </SubFormItem>
                    </SubFormItem>
                </>
            )}
        </Form>
    );
}
