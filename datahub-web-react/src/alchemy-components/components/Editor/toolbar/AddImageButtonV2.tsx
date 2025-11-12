import { Image } from '@phosphor-icons/react';
import { useCommands } from '@remirror/react';
import { Form } from 'antd';
import { FormInstance } from 'antd/es/form/Form';
import React, { useState } from 'react';
import styled from 'styled-components';

import { Button } from '@components/components/Button';
import { Dropdown } from '@components/components/Dropdown';
import { CommandButton } from '@components/components/Editor/toolbar/CommandButton';
import { FileUploadContent } from '@components/components/Editor/toolbar/FileUploadContent';
import { Input } from '@components/components/Input';

import ButtonTabs from '@app/homeV3/modules/shared/ButtonTabs/ButtonTabs';
import { colors } from '@src/alchemy-components/theme';

const UPLOAD_FILE_KEY = 'uploadFile';
const URL_KEY = 'url';

const ContentWrapper = styled.div`
    width: 300px;
    background-color: ${colors.white};
    box-shadow: 0 4px 12px 0 rgba(9, 1, 61, 0.12);
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
                <Input label="Image URL" placeholder="http://www.example.com/image.jpg" autoFocus />
            </FormItem>
            <FormItem name="alt">
                <Input label="Alt Text" />
            </FormItem>
            <StyledButton onClick={handleOk}>Embed Image</StyledButton>
        </Form>
    );
}

export const AddImageButtonV2 = () => {
    const [showDropdown, setShowDropdown] = useState(false);
    const [form] = Form.useForm();

    const tabs = [
        {
            key: UPLOAD_FILE_KEY,
            label: 'Upload File',
            content: <FileUploadContent hideDropdown={() => setShowDropdown(false)} />,
        },
        {
            key: URL_KEY,
            label: 'URL',
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
                    icon={<Image size={20} color={colors.gray[1800]} />}
                    commandName="insertImage"
                    onClick={handleButtonClick}
                />
            </Dropdown>
        </>
    );
};
