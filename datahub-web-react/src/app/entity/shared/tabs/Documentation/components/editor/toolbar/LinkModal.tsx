import { FromToProps } from '@remirror/core-types';
import { getMarkRange } from '@remirror/core-utils';
import { useAttrs, useCommands, useEditorState, useHelpers } from '@remirror/react';
import { Form, Input, Modal, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

const LINK_URL_PLACEHOLDER = 'https://www.google.com';

type LinkModalProps = {
    open: boolean;
    handleClose: () => void;
};

export const LinkModal = (props: LinkModalProps) => {
    const { open, handleClose } = props;
    const { t } = useTranslation('entity.profile.editor');
    const { t: tc } = useTranslation('common.actions');

    const [trPos, setTrPos] = useState<FromToProps>({ from: 0, to: 0 });
    const [form] = Form.useForm();

    const commands = useCommands();
    const helpers = useHelpers();
    const editorState = useEditorState();

    const href = (useAttrs().link()?.href as string) ?? '';

    useEffect(() => {
        if (open) {
            const { from, to } = editorState.selection;
            const pos = getMarkRange(editorState.doc.resolve(from), 'link') || { from, to };

            form.setFieldsValue({
                href,
                text: helpers.getTextBetween(pos.from, pos.to, editorState.doc) || '',
            });

            setTrPos(pos);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [open]);

    const handleOk = async () => {
        try {
            const values = await form.validateFields();
            const displayText = values.text || values.href;

            commands.replaceText({
                content: displayText,
                selection: trPos,
                type: 'link',
                attrs: {
                    href: values.href,
                },
            });

            form.resetFields();
            handleClose();
        } catch (e) {
            console.log('Validate Failed:', e);
        }
    };

    return (
        <Modal title={t('link.title')} okText={tc('save')} onCancel={handleClose} onOk={handleOk} open={open}>
            <Form
                form={form}
                layout="vertical"
                colon={false}
                requiredMark={false}
                onKeyPress={(e) => e.key === 'Enter' && form.submit()}
            >
                <Form.Item
                    name="href"
                    label={<Typography.Text strong>{t('link.linkUrl')}</Typography.Text>}
                    rules={[{ required: true }]}
                >
                    <Input placeholder={LINK_URL_PLACEHOLDER} autoFocus />
                </Form.Item>
                <Form.Item name="text" label={<Typography.Text strong>{t('link.text')}</Typography.Text>}>
                    <Input />
                </Form.Item>
            </Form>
        </Modal>
    );
};
