import { Input } from '@components';
import { Form } from 'antd';
import React, { useCallback, useEffect, useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { UploadFileForm } from '@app/entityV2/shared/components/links/UploadFileForm';
import { UrlLinkForm } from '@app/entityV2/shared/components/links/UrlLinkForm';
import { LinkFormData, LinkFormVariant } from '@app/entityV2/shared/components/links/types';
import ButtonTabs from '@app/homeV3/modules/shared/ButtonTabs/ButtonTabs';
import { useIsDocumentationFileUploadV1Enabled } from '@app/shared/hooks/useIsDocumentationFileUploadV1Enabled';

interface Props {
    initialValues?: Partial<LinkFormData>;
}

const TAB_KEY_URL = 'URL';
const TAB_KEY_UPLOAD_FILE = 'uploadFile';

export function LinkFormWrapper({ initialValues }: Props) {
    const { t } = useTranslation('entity.shared.components');
    const { t: tc } = useTranslation('common.actions');
    const isDocumentationFileUploadV1Enabled = useIsDocumentationFileUploadV1Enabled();

    const form = Form.useFormInstance<LinkFormData>();
    const setVariant = useCallback(
        (variant: LinkFormVariant) => {
            form.setFieldValue('variant', variant);
            form.validateFields(['variant']);
        },
        [form],
    );
    const setLabel = useCallback(
        (newLabel: string) => {
            form.setFieldValue('label', newLabel);
            form.validateFields(['label']);
        },
        [form],
    );

    const defaultTabKey = useMemo(() => {
        if (initialValues?.variant === LinkFormVariant.URL) {
            return TAB_KEY_URL;
        }

        return TAB_KEY_UPLOAD_FILE;
    }, [initialValues?.variant]);

    useEffect(() => {
        const defaultVariant = isDocumentationFileUploadV1Enabled ? LinkFormVariant.UploadFile : LinkFormVariant.URL;
        setVariant(initialValues?.variant ?? defaultVariant);
    }, [initialValues?.variant, setVariant, isDocumentationFileUploadV1Enabled]);

    const onTabChanged = useCallback(
        (key: string) => {
            if (key === TAB_KEY_UPLOAD_FILE) {
                form.setFieldValue('variant', LinkFormVariant.UploadFile);
            } else if (key === TAB_KEY_URL) {
                form.setFieldValue('variant', LinkFormVariant.URL);
            }
        },
        [form],
    );

    const tabs = useMemo(
        () => [
            {
                key: TAB_KEY_UPLOAD_FILE,
                label: tc('upload'),
                content: <UploadFileForm initialValues={initialValues} />,
            },
            {
                key: TAB_KEY_URL,
                label: 'URL',
                content: <UrlLinkForm />,
            },
        ],
        [initialValues, tc],
    );

    return (
        <>
            <Form.Item
                data-testid="link-form-modal-variant"
                name="variant"
                initialValue={initialValues?.variant}
                hidden
            />

            {isDocumentationFileUploadV1Enabled ? (
                <ButtonTabs tabs={tabs} onTabClick={onTabChanged} defaultKey={defaultTabKey} />
            ) : (
                <UrlLinkForm />
            )}

            <Form.Item
                name="label"
                rules={[
                    {
                        required: true,
                        message: t('links.labelRequired'),
                    },
                ]}
            >
                <Input
                    label={t('links.fieldLabel')}
                    placeholder={t('links.fieldLabelPlaceholder')}
                    inputTestId="label-input"
                    onClear={() => setLabel('')}
                    isRequired
                />
            </Form.Item>
        </>
    );
}
