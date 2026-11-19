import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { Editor } from '@app/sharedV2/editor/Editor';
import { Input } from '@src/alchemy-components';
import { Label } from '@src/alchemy-components/components/Input/components';

export type PostFormData = {
    title: string;
    description: string;
};

const FormField = styled.div`
    margin-bottom: 24px;
`;

const StyledEditor = styled(Editor)`
    border: 1px solid ${(props) => props.theme.colors.border};
    margin-top: 8px;
`;

type Props = {
    formData: PostFormData;
    onChange: (field: keyof PostFormData, value: string) => void;
};

export default function CreatePostForm({ formData, onChange }: Props) {
    const { t } = useTranslation('settings.posts');

    return (
        <form onSubmit={(e) => e.preventDefault()}>
            <FormField>
                <Input
                    label={t('titleLabel')}
                    inputTestId="create-post-title"
                    placeholder={t('titlePlaceholder')}
                    value={formData.title}
                    setValue={(val) => onChange('title', val)}
                    isRequired
                />
            </FormField>

            <FormField>
                <Label>{t('descriptionLabel')}</Label>
                <StyledEditor
                    className="create-post-description"
                    doNotFocus
                    content={formData.description || undefined}
                    onChange={(val) => onChange('description', val)}
                />
            </FormField>
        </form>
    );
}
