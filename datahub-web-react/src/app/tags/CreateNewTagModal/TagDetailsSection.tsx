import { ColorPicker, Input } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

// Tag details section props
interface TagDetailsProps {
    tagName: string;
    setTagName: React.Dispatch<React.SetStateAction<string>>;
    tagDescription: string;
    setTagDescription: React.Dispatch<React.SetStateAction<string>>;
    tagColor: string;
    setTagColor: (color: string) => void;
}

const SectionContainer = styled.div`
    margin-bottom: 24px;
`;

const FormSection = styled.div`
    margin-bottom: 16px;
`;

/**
 * Component for tag name, description, and color selection
 */
const TagDetailsSection: React.FC<TagDetailsProps> = ({
    tagName,
    setTagName,
    tagDescription,
    setTagDescription,
    tagColor,
    setTagColor,
}) => {
    const { t } = useTranslation('misc');
    const { t: tc } = useTranslation('common.labels');
    const handleTagNameChange: React.Dispatch<React.SetStateAction<string>> = (value) => {
        if (typeof value === 'function') {
            setTagName(value);
        } else {
            setTagName(value);
        }
    };

    const handleDescriptionChange: React.Dispatch<React.SetStateAction<string>> = (value) => {
        if (typeof value === 'function') {
            setTagDescription(value);
        } else {
            setTagDescription(value);
        }
    };

    const handleColorChange = (color: string) => {
        setTagColor(color);
    };

    return (
        <SectionContainer>
            <FormSection>
                <Input
                    label={tc('name')}
                    value={tagName}
                    setValue={handleTagNameChange}
                    placeholder={t('tags.namePlaceholder')}
                    data-testid="tag-name-field"
                    required
                />
            </FormSection>

            <FormSection>
                <Input
                    label={tc('description')}
                    value={tagDescription}
                    setValue={handleDescriptionChange}
                    placeholder={t('tags.descriptionPlaceholder')}
                    data-testid="tag-description-field"
                    type="textarea"
                />
            </FormSection>

            <FormSection>
                <ColorPicker initialColor={tagColor} onChange={handleColorChange} label={t('tags.color')} />
            </FormSection>
        </SectionContainer>
    );
};

export default TagDetailsSection;
