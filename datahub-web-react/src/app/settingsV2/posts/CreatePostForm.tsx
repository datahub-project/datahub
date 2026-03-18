import { Editor } from '@components';
import React from 'react';
import styled from 'styled-components';

import { TabButtons } from '@app/homeV3/modules/shared/ButtonTabs/TabButtons';
import { Input } from '@src/alchemy-components';
import { Label } from '@src/alchemy-components/components/Input/components';

import { PostContentType } from '@types';

export type PostFormData = {
    type: PostContentType;
    title: string;
    description: string;
    link: string;
    location: string;
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
    onChange: (field: keyof PostFormData, value: string | PostContentType) => void;
};

export default function CreatePostForm({ formData, onChange }: Props) {
    return (
        <div>
            <FormField>
                <Label>Content Type</Label>
                <TabButtons
                    tabs={[
                        { key: PostContentType.Text, label: 'Announcement', content: null },
                        { key: PostContentType.Link, label: 'Pinned Link', content: null },
                    ]}
                    activeTab={formData.type}
                    onTabClick={(key) => onChange('type', key as PostContentType)}
                />
            </FormField>

            <FormField>
                <Input
                    label="Title"
                    inputTestId="create-post-title"
                    placeholder="Your title"
                    value={formData.title}
                    setValue={(val) => onChange('title', val)}
                    isRequired
                />
            </FormField>

            {formData.type === PostContentType.Text && (
                <FormField>
                    <Label>Description</Label>
                    <StyledEditor
                        className="create-post-description"
                        doNotFocus
                        content={formData.description || undefined}
                        onChange={(val) => onChange('description', val)}
                    />
                </FormField>
            )}

            {formData.type === PostContentType.Link && (
                <>
                    <FormField>
                        <Input
                            label="Link URL"
                            inputTestId="create-post-link"
                            placeholder="Your link URL"
                            value={formData.link}
                            setValue={(val) => onChange('link', val)}
                        />
                    </FormField>

                    <FormField>
                        <Input
                            label="Image URL"
                            inputTestId="create-post-media-location"
                            placeholder="Your image URL"
                            value={formData.location}
                            setValue={(val) => onChange('location', val)}
                        />
                    </FormField>

                    <FormField>
                        <Label>Description</Label>
                        <StyledEditor
                            doNotFocus
                            content={formData.description || undefined}
                            onChange={(val) => onChange('description', val)}
                            className="create-post-description"
                        />
                    </FormField>
                </>
            )}
        </div>
    );
}
