import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { LogoCountCard } from '../../shared/LogoCountCard';
import { IngestionSourceBuilderStep, SourceType, StepProps, TemplateBuilderState } from './types';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
`;

const PlatformListContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
    flex-wrap: wrap;
`;

const SelectTemplateHeader = styled(Typography.Title)`
    && {
        margin-bottom: 16px;
    }
`;

const InlineTextButton = styled(Button)`
    && {
        margin: 0px;
        padding: 0px;
    }
`;

// TODO: Resolve the name, logo of supported platforms dynamically
const platforms = [
    {
        urn: 'urn:li:dataPlatform:bigquery',
        type: SourceType.BIGQUERY,
        name: 'BigQuery',
        logoUrl:
            'https://raw.githubusercontent.com/linkedin/datahub/master/datahub-web-react/src/images/bigquerylogo.png',
    },
];

/**
 * Component responsible for selecting the mechanism for constructing a new Ingestion Source
 */
export const SelectTemplateStep = ({ state, updateState, goTo }: StepProps) => {
    // Reoslve the supported platform types to their logos and names.

    const onSelectTemplate = (sourceType: SourceType) => {
        const newState: TemplateBuilderState = {
            ...state,
            source: {
                type: sourceType, // Obviously resolve the correct ht,
                configs: {},
            },
        };
        updateState(newState);
        goTo(IngestionSourceBuilderStep.CONFIGURE_SOURCE_TEMPLATE);
    };

    const onClickBuildRecipe = () => {
        // No state changes to make. Simply goTo.
        goTo(IngestionSourceBuilderStep.DEFINE_RECIPE);
    };

    return (
        <>
            <Section>
                <SelectTemplateHeader level={5}>Select a template</SelectTemplateHeader>
                <PlatformListContainer>
                    {platforms.map((platform, _) => (
                        <LogoCountCard
                            key={platform.urn}
                            onClick={() => onSelectTemplate(platform.type)}
                            name={platform.name}
                            logoUrl={platform.logoUrl}
                        />
                    ))}
                </PlatformListContainer>
            </Section>
            <Section>
                <span>
                    Or,{' '}
                    <InlineTextButton type="link" onClick={onClickBuildRecipe}>
                        build a recipe manually.
                    </InlineTextButton>
                </span>
            </Section>
        </>
    );
};
