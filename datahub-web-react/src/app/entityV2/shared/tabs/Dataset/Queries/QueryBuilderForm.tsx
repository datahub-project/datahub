import Editor from '@monaco-editor/react';
import { Form, Input, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import InferDocsPanel from '@app/entityV2/shared/components/inferredDocs/InferDocsPanel';
import { useShouldShowInferDocumentationButton } from '@app/entityV2/shared/components/inferredDocs/utils';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { QueryBuilderState } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import { Editor as MarkdownEditor } from '@app/entityV2/shared/tabs/Documentation/components/editor/Editor';

import { EntityType } from '@types';

const EditorWrapper = styled.div`
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 1px;
    background-color: ${ANTD_GRAY[2]};
`;

const StyledEditor = styled(MarkdownEditor)`
    border: 1px solid ${ANTD_GRAY[4.5]};
`;

const QUERY_EDITOR_HEIGHT = '240px';

const QUERY_EDITOR_OPTIONS = {
    minimap: { enabled: false },
    scrollbar: {
        vertical: 'hidden',
        horizontal: 'hidden',
    },
} as any;

type Props = {
    state: QueryBuilderState;
    updateState: (newState: QueryBuilderState) => void;
};

export default function QueryBuilderForm({ state, updateState }: Props) {
    // Key to force re-render of the editor when the description should be updated.
    const [editorKey, setEditorKey] = useState(0);
    const shouldShowInferenceButton = useShouldShowInferDocumentationButton(EntityType.Query);

    const updateQuery = (query) => {
        updateState({
            ...state,
            query,
        });
    };

    const updateTitle = (title) => {
        updateState({
            ...state,
            title,
        });
    };

    const updateDescription = (description) => {
        console.log(description);
        updateState({
            ...state,
            description,
        });
    };

    return (
        <Form layout="vertical">
            <Form.Item required label={<Typography.Text strong>Query</Typography.Text>}>
                <EditorWrapper>
                    <Editor
                        options={QUERY_EDITOR_OPTIONS}
                        height={QUERY_EDITOR_HEIGHT}
                        defaultLanguage="sql"
                        value={state.query}
                        onChange={updateQuery}
                        className="query-builder-editor-input"
                    />
                </EditorWrapper>
            </Form.Item>
            <Form.Item
                rules={[{ min: 1, max: 500 }]}
                hasFeedback
                label={<Typography.Text strong>Title</Typography.Text>}
            >
                <Input
                    data-testid="query-builder-title-input"
                    autoFocus
                    value={state.title}
                    onChange={(newTitle) => updateTitle(newTitle.target.value)}
                    placeholder="Join Transactions and Users Tables"
                />
            </Form.Item>
            <Form.Item label={<Typography.Text strong>Description</Typography.Text>}>
                <StyledEditor
                    key={editorKey}
                    data-testid="query-builder-description-input"
                    doNotFocus
                    content={state.description}
                    onChange={updateDescription}
                />
            </Form.Item>
            {shouldShowInferenceButton && state.urn && (
                <InferDocsPanel
                    urn={state.urn}
                    insertText="Insert"
                    showInsert
                    onInsertDescription={(description) => {
                        updateDescription(description);
                        setEditorKey(editorKey + 1); // Force the description editor to refresh
                    }}
                    surface="query-builder-form"
                />
            )}
        </Form>
    );
}
