import { Button, message, Skeleton, Typography } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';
import { Sparkle } from 'phosphor-react';
import analytics, { EventType, InferDocsClickEvent } from '@src/app/analytics';
import { ANTD_GRAY } from '../../constants';
import { useInferDocumentationForItem, useIsDocumentationInferenceEnabled } from './utils';
import { Editor } from '../../tabs/Documentation/components/editor/Editor';
import InferDocsButton from './InferDocsButton';

const InferencePanelContainer = styled.div`
    padding: 16px;
    background-color: ${ANTD_GRAY[2]};
    border-radius: 8px;
`;

const AiSparkle = styled(Sparkle)`
    height: 16px;
    width: 16px;
    margin-right: 4px;
`;

const InferencePanelHeader = styled.div`
    display: flex;
    align-items: center;
    padding-bottom: 4px;
    padding-top: 4px;
    margin-bottom: 8px;
    color: #5c3fd1;
    font-size: 14px;
    font-weight: 600;
`;

const InferencePanelBody = styled.div`
    margin-bottom: 8px;

    .remirror-editor.ProseMirror {
        padding: 0px !important;
    }
`;

const InferencePanelDescription = styled(Typography.Text)`
    font-size: 14px;
    color: ${ANTD_GRAY[8]};
`;

const InferencePanelFooter = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: flex-end;
    align-items: center;
`;

const CollapseButton = styled.button`
    padding: 8px;
    cursor: pointer;
    margin-right: 12px;
    background-color: transparent;
    border: 0;
    font-weight: 600;
`;

const InsertButton = styled(Button)`
    background-color: #f1f3fd;
    cursor: pointer;
    color: #5c3fd1;
    box-shadow: none;
    border-width: 0.5px;
    &:hover {
        background-color: #f1f3fd;
    }
`;

type Props = {
    urn: string;
    buttonText?: string;
    insertText?: string;
    showInsert?: boolean;
    collapseOnInsert?: boolean;
    onInsertDescription: (desc: string) => void;
    inferOnMount?: boolean;
    forColumnPath?: string;
    // For analytics tracking
    surface?: InferDocsClickEvent['surface'];
};

export default function InferDocsPanel({
    urn,
    buttonText,
    insertText = 'Insert',
    showInsert = true,
    collapseOnInsert = true,
    onInsertDescription,
    inferOnMount,
    forColumnPath,
    surface,
}: Props) {
    const [isPanelExpanded, setIsPanelExpanded] = useState(false);
    const [isLoading, setIsLoading] = useState(false);
    const [generatedDescription, setGeneratedDescription] = useState('');
    const [hasInserted, setHasInserted] = useState(false);

    const enableDocInference = useIsDocumentationInferenceEnabled();

    const inferDocumentationForItem = useInferDocumentationForItem({
        entityUrn: urn,
        columnPath: forColumnPath,
    });

    const generateSuggestion = async () => {
        setIsLoading(true);
        setIsPanelExpanded(true);
        try {
            const result = await inferDocumentationForItem();
            if (!result) {
                throw new Error(`Could not generate a description for ${forColumnPath ?? 'this asset'}.`);
            }
            setGeneratedDescription(result);
        } catch (e: unknown) {
            const error = e as Error;
            console.error(error);
            message.error({ content: `Failed to generate summary. ${error.message}`, duration: 3 });
            setIsPanelExpanded(false);
        } finally {
            setIsLoading(false);
        }
    };

    const onClose = () => {
        analytics.event({ type: EventType.DeclineInferredDocs });
        if (collapseOnInsert) {
            setIsPanelExpanded(false);
        }
        setHasInserted(true);
    };

    // Wrapping in ref so method doesn't need to be a dependency of the useEffect
    const generateSuggestionRef = useRef(generateSuggestion);
    generateSuggestionRef.current = generateSuggestion;
    useEffect(() => {
        if (inferOnMount) {
            generateSuggestionRef.current();
        }
    }, [inferOnMount]);

    // --------------------------------- render --------------------------------- //
    if (!enableDocInference) return null;

    return isPanelExpanded ? (
        <InferencePanelContainer>
            <InferencePanelHeader>
                <AiSparkle />
                {isLoading ? 'Generating...' : 'Generated'}
            </InferencePanelHeader>
            <InferencePanelBody>
                {isLoading && <Skeleton active title={false} paragraph={{ rows: 3 }} />}
                {generatedDescription && !isLoading && (
                    <InferencePanelDescription>
                        <Editor className="inferred-documentation-editor" content={generatedDescription} readOnly />
                    </InferencePanelDescription>
                )}
            </InferencePanelBody>
            <InferencePanelFooter>
                <CollapseButton type="button" onClick={onClose}>
                    Dismiss
                </CollapseButton>
                {showInsert && (
                    <InsertButton
                        disabled={isLoading || hasInserted}
                        onClick={() => {
                            analytics.event({ type: EventType.AcceptInferredDocs });
                            onInsertDescription(`\n\n\n${generatedDescription}`);
                            onClose();
                        }}
                    >
                        {insertText}
                    </InsertButton>
                )}
            </InferencePanelFooter>
        </InferencePanelContainer>
    ) : (
        <InferDocsButton surface={surface} title={buttonText} onClick={generateSuggestion} />
    );
}
