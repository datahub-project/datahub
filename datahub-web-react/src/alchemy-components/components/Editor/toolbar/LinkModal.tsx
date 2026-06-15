import { FromToProps } from '@remirror/core-types';
import { getMarkRange } from '@remirror/core-utils';
import { useAttrs, useCommands, useEditorState, useHelpers } from '@remirror/react';
import { Form, Input, Typography } from 'antd';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';

import { Modal } from '@components/components/Modal';

// Sample URL shown as input placeholder — illustrative, not user-facing copy.
const EXAMPLE_LINK_URL = 'https://www.google.com';

type LinkModalProps = {
    visible: boolean;
    handleClose: () => void;
};

export const LinkModal = (props: LinkModalProps) => {
    const { visible, handleClose } = props;
    const { t } = useTranslation('alchemy');
    const { t: tc } = useTranslation('common.actions');

    const [trPos, setTrPos] = useState<FromToProps>({ from: 0, to: 0 });
    const [form] = Form.useForm();

    const commands = useCommands();
    const helpers = useHelpers();
    const editorState = useEditorState();

    const href = (useAttrs().link()?.href as string) ?? '';

    useEffect(() => {
        if (visible) {
            const { from, to } = editorState.selection;
            const pos = getMarkRange(editorState.doc.resolve(from), 'link') || { from, to };

            form.setFieldsValue({
                href,
                text: helpers.getTextBetween(pos.from, pos.to, editorState.doc) || '',
            });

            setTrPos(pos);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [visible]);

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
        <Modal
            title={t('editor.link.title')}
            onCancel={handleClose}
            open={visible}
            buttons={[
                {
                    text: tc('save'),
                    variant: 'filled',
                    onClick: handleOk,
                },
            ]}
            zIndex={1200}
        >
            <Form
                form={form}
                layout="vertical"
                colon={false}
                requiredMark={false}
                onKeyPress={(e) => e.key === 'Enter' && form.submit()}
            >
                <Form.Item
                    name="href"
                    label={<Typography.Text strong>{t('editor.link.urlLabel')}</Typography.Text>}
                    rules={[{ required: true }]}
                >
                    <Input placeholder={EXAMPLE_LINK_URL} autoFocus />
                </Form.Item>
                <Form.Item name="text" label={<Typography.Text strong>{t('editor.link.textLabel')}</Typography.Text>}>
                    <Input />
                </Form.Item>
            </Form>
        </Modal>
    );
};
