import { Input } from '@components';
import Editor from '@monaco-editor/react';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { QueryBuilderState } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import { SQL_LANGUAGE } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/constants';
import { useMonacoTheme } from '@app/theme/useMonacoTheme';
import { Editor as MarkdownEditor } from '@src/alchemy-components';

const EditorWrapper = styled.div`
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 1px;
    background-color: ${(props) => props.theme.colors.bgSurface};
`;

const StyledEditor = styled(MarkdownEditor)`
    border: 1px solid ${(props) => props.theme.colors.border};
`;

const Form = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
`;

const Field = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const FieldLabel = styled.label`
    color: ${(props) => props.theme.colors.text};
    font-weight: 600;
`;

const Required = styled.span`
    color: ${(props) => props.theme.colors.textError};
`;

const QUERY_EDITOR_HEIGHT = '240px';

const TITLE_MAX_LENGTH = 500;

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
    const { t } = useTranslation('entity.profile.queries');
    const { t: tc } = useTranslation('common.labels');
    const monacoTheme = useMonacoTheme();

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
        updateState({
            ...state,
            description,
        });
    };

    return (
        <Form>
            <Field>
                <FieldLabel>
                    {t('queryBuilderModal.formLabelQuery')} <Required>*</Required>
                </FieldLabel>
                <EditorWrapper>
                    <Editor
                        {...monacoTheme}
                        options={QUERY_EDITOR_OPTIONS}
                        height={QUERY_EDITOR_HEIGHT}
                        defaultLanguage={SQL_LANGUAGE}
                        value={state.query}
                        onChange={updateQuery}
                        className="query-builder-editor-input"
                    />
                </EditorWrapper>
            </Field>
            <Input
                inputTestId="query-builder-title-input"
                autoFocus
                value={state.title}
                setValue={updateTitle}
                label={t('queryBuilderModal.formLabelTitle')}
                placeholder={t('queryBuilderModal.titlePlaceholder')}
                maxLength={TITLE_MAX_LENGTH}
            />
            <Field>
                <FieldLabel>{tc('description')}</FieldLabel>
                <StyledEditor
                    data-testid="query-builder-description-input"
                    doNotFocus
                    content={state.description}
                    onChange={updateDescription}
                />
            </Field>
        </Form>
    );
}
