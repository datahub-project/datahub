import { Pill, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { AiContext } from '@types';

const ContentWrapper = styled.div`
    padding: 4px 8px 0;
`;

const Section = styled.div`
    margin-bottom: 12px;
`;

const SectionTitle = styled(Text).attrs({ weight: 'semiBold', size: 'sm' })`
    margin-bottom: 6px;
    display: block;
`;

const SynonymTags = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
`;

const ExampleList = styled.ul`
    margin: 0;
    padding-left: 20px;
`;

const ExampleItem = styled.li`
    margin-bottom: 4px;
`;

type EntityDataWithAiContext = {
    info?: {
        aiContext?: AiContext | null;
    } | null;
};

export default function AiContextModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { entityData } = useEntityData();

    const aiContext = (entityData as EntityDataWithAiContext)?.info?.aiContext;

    if (!aiContext) {
        return null;
    }

    const { synonyms, instructions, examples } = aiContext;
    const hasSynonyms = synonyms && synonyms.length > 0;
    const hasInstructions = !!instructions;
    const hasExamples = examples && examples.length > 0;

    if (!hasSynonyms && !hasInstructions && !hasExamples) {
        return null;
    }

    return (
        <LargeModule {...props} dataTestId="ai-context-module">
            <ContentWrapper>
                {hasSynonyms && (
                    <Section>
                        <SectionTitle>{t('aiContext.synonymsTitle')}</SectionTitle>
                        <SynonymTags>
                            {synonyms.map((synonym) => (
                                <Pill key={synonym} label={synonym} />
                            ))}
                        </SynonymTags>
                    </Section>
                )}
                {hasInstructions && (
                    <Section>
                        <SectionTitle>{t('aiContext.instructionsTitle')}</SectionTitle>
                        <Text size="sm">{instructions}</Text>
                    </Section>
                )}
                {hasExamples && (
                    <Section>
                        <SectionTitle>{t('aiContext.examplesTitle')}</SectionTitle>
                        <ExampleList>
                            {examples.map((example) => (
                                <ExampleItem key={example}>
                                    <Text size="sm">{example}</Text>
                                </ExampleItem>
                            ))}
                        </ExampleList>
                    </Section>
                )}
            </ContentWrapper>
        </LargeModule>
    );
}
