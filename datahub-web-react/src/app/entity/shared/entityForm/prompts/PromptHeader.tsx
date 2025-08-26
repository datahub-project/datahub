import React from 'react';
import styled from 'styled-components';

import { useEntityFormContext } from '@app/entity/shared/entityForm/EntityFormContext';
import { Editor } from '@app/entity/shared/tabs/Documentation/components/editor/Editor';

const PromptTitle = styled.div<{ displayBulkStyles?: boolean }>`
    font-size: 16px;
    font-weight: 600;
    line-height: 20px;
    ${(props) => props.displayBulkStyles && `font-size: 20px;`}
`;

const RequiredText = styled.span<{ displayBulkStyles?: boolean }>`
    font-size: 12px;
    margin-left: 4px;
    color: #a8071a;
    ${(props) =>
        props.displayBulkStyles &&
        `
        margin-left: 8px;
    `}
`;

const PromptSubTitle = styled.div`
    font-size: 14px;
    font-weight: 500;
    line-height: 18px;
    margin-top: 4px;
`;

interface Props {
    title: string;
    description?: string | null;
    promptNumber?: number;
    required: boolean;
}

export default function PromptHeader({ title, description, promptNumber, required }: Props) {
    const {
        prompt: { displayBulkPromptStyles },
    } = useEntityFormContext();
    return (
        <>
            <PromptTitle displayBulkStyles={displayBulkPromptStyles} data-testid="prompt-title">
                {promptNumber !== undefined && <>{promptNumber}. </>}
                {title}
                {required && (
                    <RequiredText displayBulkStyles={displayBulkPromptStyles} data-testid="prompt-required">
                        required
                    </RequiredText>
                )}
            </PromptTitle>
            {description && (
                <PromptSubTitle>
                    <Editor content={description} readOnly editorStyle="padding: 0;" />
                </PromptSubTitle>
            )}
        </>
    );
}
