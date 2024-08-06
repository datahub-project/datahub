import { Button, message, Skeleton, Typography } from 'antd';
import React, { useEffect, useRef, useState } from 'react';
import styled from 'styled-components';
import { Sparkle } from 'phosphor-react';
import { useEntityData } from '@src/app/entity/shared/EntityContext';

import { ANTD_GRAY } from '../../constants';
import { useInferDocumentationForItem, useIsDocumentationInferenceEnabled } from './utils';

const InferencePanelContainer = styled.div`
    padding: 16px;
    background-color: ${ANTD_GRAY[2]};
    border-radius: 8px;
`;
const GenerateButton = styled.button`
    background-color: #f1f3fd;
    color: #5c3fd1;
    width: 140px;
    height: 40px;
    justify-content: center;
    align-items: center;
    display: flex;
    border-radius: 8px;
    cursor: pointer;
    border: 0;
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
    margin-bottom: 24px;
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
    onInsertDescription: (desc: string) => void;
    inferOnMount?: boolean;
    forColumnPath?: string;
};

export default function InferDocsPanel({ onInsertDescription, forColumnPath, inferOnMount }: Props) {
    const { urn: entityUrn } = useEntityData();
    const [isPanelExpanded, setIsPanelExpanded] = useState(false);
    const [isLoading, setIsLoading] = useState(false);
    const [generatedDescription, setGeneratedDescription] = useState('');

    const enableDocInference = useIsDocumentationInferenceEnabled();

    const inferDocumentationForItem = useInferDocumentationForItem({
        entityUrn,
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
            message.error({ content: `Failed to generate: \n ${error.message || ''}`, duration: 3 });
            setIsPanelExpanded(false);
        } finally {
            setIsLoading(false);
        }
    };
    const onClose = () => {
        setIsPanelExpanded(false);
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
                    <InferencePanelDescription>{generatedDescription}</InferencePanelDescription>
                )}
            </InferencePanelBody>
            <InferencePanelFooter>
                <CollapseButton type="button" onClick={onClose}>
                    Dismiss
                </CollapseButton>
                <InsertButton
                    disabled={isLoading}
                    onClick={() => {
                        onInsertDescription(`\n\n\n${generatedDescription}`);
                        onClose();
                    }}
                >
                    Insert
                </InsertButton>
            </InferencePanelFooter>
        </InferencePanelContainer>
    ) : (
        <GenerateButton type="button" onClick={() => generateSuggestion()}>
            <AiSparkle />
            Generate with AI
        </GenerateButton>
    );
}
