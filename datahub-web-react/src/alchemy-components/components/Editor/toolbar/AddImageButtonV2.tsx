import { Image } from '@phosphor-icons/react/dist/csr/Image';
import { useCommands } from '@remirror/react';
import { Form } from 'antd';
import { FormInstance } from 'antd/es/form/Form';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import { Button } from '@components/components/Button';
import { Dropdown } from '@components/components/Dropdown';
import { CommandButton } from '@components/components/Editor/toolbar/CommandButton';
import { FileUploadContent } from '@components/components/Editor/toolbar/FileUploadContent';
import { Input } from '@components/components/Input';

import ButtonTabs from '@app/homeV3/modules/shared/ButtonTabs/ButtonTabs';

const UPLOAD_FILE_KEY = 'uploadFile';
const URL_KEY = 'url';

// Sample URL shown as input placeholder — illustrative, not user-facing copy.
const EXAMPLE_IMAGE_URL = 'http://www.example.com/image.jpg';

const ContentWrapper = styled.div`
    width: 300px;
    background-color: ${({ theme }) => theme.colors.bg};
    box-shadow: ${({ theme }) => theme.colors.shadowMd};
    display: flex;
    flex-direction: column;
    padding: 8px;
    gap: 8px;
    border-radius: 12px;
`;

const StyledButton = styled(Button)`
    width: 100%;
    display: flex;
    justify-content: center;
`;

const FormItem = styled(Form.Item)`
    margin-bottom: 8px;
`;

function ImageUrlInput({ form, hideDropdown }: { form: FormInstance<any>; hideDropdown: () => void }) {
    const { t } = useTranslation('alchemy');
    const { insertImage } = useCommands();

    const handleOk = () => {
        form.validateFields()
            .then((values) => {
                form.resetFields();
                hideDropdown();
                insertImage(values);
            })
            .catch((info) => {
                console.log('Validate Failed:', info);
            });
    };

    return (
        <Form form={form} layout="vertical" colon={false} requiredMark={false}>
            <FormItem name="src" rules={[{ required: true }]}>
                <Input label={t('editor.addImage.urlLabel')} placeholder={EXAMPLE_IMAGE_URL} autoFocus />
            </FormItem>
            <FormItem name="alt">
                <Input label={t('editor.addImage.altLabel')} />
            </FormItem>
            <StyledButton onClick={handleOk}>{t('editor.addImage.embed')}</StyledButton>
        </Form>
    );
}

export const AddImageButtonV2 = () => {
    const { t } = useTranslation('alchemy');
    const [showDropdown, setShowDropdown] = useState(false);
    const [form] = Form.useForm();
    const styledTheme = useTheme();
    const iconColor = styledTheme.colors.icon;

    const tabs = [
        {
            key: UPLOAD_FILE_KEY,
            label: t('editor.upload.uploadFile'),
            content: <FileUploadContent hideDropdown={() => setShowDropdown(false)} />,
        },
        {
            key: URL_KEY,
            label: t('editor.upload.tabUrl'),
            content: <ImageUrlInput form={form} hideDropdown={() => setShowDropdown(false)} />,
        },
    ];

    const handleButtonClick = () => {
        setShowDropdown(true);
    };

    const dropdownContent = () => (
        <ContentWrapper>
            <ButtonTabs tabs={tabs} defaultKey={UPLOAD_FILE_KEY} />
        </ContentWrapper>
    );

    return (
        <>
            <Dropdown
                open={showDropdown}
                onOpenChange={(open) => setShowDropdown(open)}
                dropdownRender={dropdownContent}
            >
                <CommandButton
                    active={false}
                    icon={<Image size={20} color={iconColor} />}
                    commandName="insertImage"
                    onClick={handleButtonClick}
                />
            </Dropdown>
        </>
    );
};
