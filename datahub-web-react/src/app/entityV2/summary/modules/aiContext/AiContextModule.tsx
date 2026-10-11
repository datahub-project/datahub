import { Pill, Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import ModulePillRow from '@app/homeV3/module/components/ModulePillRow';
import { ModuleProps } from '@app/homeV3/module/types';

import { AiContext } from '@types';

const ContentWrapper = styled.div`
    padding: 4px 8px 0;
    color: ${(props) => props.theme.colors.text};
`;

const Section = styled.div`
    margin-bottom: 12px;
`;

const SectionTitle = styled(Text).attrs({ weight: 'bold', size: 'sm' })`
    margin-bottom: 6px;
    display: block;
    color: ${(props) => props.theme.colors.text};
`;

const BodyText = styled(Text).attrs({ size: 'md', weight: 'normal' })`
    color: ${(props) => props.theme.colors.text};
`;

const ExampleList = styled.ul`
    margin: 0;
    padding-left: 20px;
    color: ${(props) => props.theme.colors.text};
`;

const ExampleItem = styled.li`
    margin-bottom: 4px;
`;

type EntityDataWithAiContext = {
    aiContext?: AiContext | null;
};

function withStableKeys(values: string[]): Array<{ value: string; key: string }> {
    const counts = new Map<string, number>();
    return values.map((value) => {
        const occurrence = counts.get(value) ?? 0;
        counts.set(value, occurrence + 1);
        return {
            value,
            key: occurrence === 0 ? value : `${value}-${occurrence}`,
        };
    });
}

export default function AiContextModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { entityData } = useEntityData();

    const aiContext = (entityData as EntityDataWithAiContext)?.aiContext;

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
                        <ModulePillRow>
                            {withStableKeys(synonyms).map(({ value: synonym, key }) => (
                                <Pill key={key} label={synonym} size="sm" clickable={false} />
                            ))}
                        </ModulePillRow>
                    </Section>
                )}
                {hasInstructions && (
                    <Section>
                        <SectionTitle>{t('aiContext.instructionsTitle')}</SectionTitle>
                        <BodyText>{instructions}</BodyText>
                    </Section>
                )}
                {hasExamples && (
                    <Section>
                        <SectionTitle>{t('aiContext.examplesTitle')}</SectionTitle>
                        <ExampleList>
                            {withStableKeys(examples).map(({ value: example, key }) => (
                                <ExampleItem key={key}>
                                    <BodyText>{example}</BodyText>
                                </ExampleItem>
                            ))}
                        </ExampleList>
                    </Section>
                )}
            </ContentWrapper>
        </LargeModule>
    );
}
