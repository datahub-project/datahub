import React from 'react';
import styled from 'styled-components';
import { Input, ColorPicker } from '@components';
import { TagDetailsProps } from './types';

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
                    label="Name"
                    value={tagName}
                    setValue={handleTagNameChange}
                    placeholder="Enter tag name"
                    required
                />
            </FormSection>

            <FormSection>
                <Input
                    label="Description"
                    value={tagDescription}
                    setValue={handleDescriptionChange}
                    placeholder="Add a description for your new tag"
                    type="textarea"
                />
            </FormSection>

            <FormSection>
                <ColorPicker initialColor={tagColor} onChange={handleColorChange} label="Color" />
            </FormSection>
        </SectionContainer>
    );
};

export default TagDetailsSection;
