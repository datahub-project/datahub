import { Typography, Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { LogoCountCard } from '../../shared/LogoCountCard';
import { BaseBuilderState, IngestionSourceBuilderStep, StepProps } from './types';
import { SOURCE_TEMPLATE_CONFIGS } from './utils';

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

/**
 * Component responsible for selecting the mechanism for constructing a new Ingestion Source
 */
export const SelectTemplateStep = ({ state, updateState, goTo, cancel }: StepProps) => {
    // Reoslve the supported platform types to their logos and names.

    const onSelectTemplate = (type: string) => {
        const newState: BaseBuilderState = {
            ...state,
            type,
        };
        updateState(newState);
        goTo(IngestionSourceBuilderStep.DEFINE_RECIPE);
    };

    return (
        <>
            <Section>
                <SelectTemplateHeader level={5}>Select a Template</SelectTemplateHeader>
                <PlatformListContainer>
                    {SOURCE_TEMPLATE_CONFIGS.map((configs, _) => (
                        <LogoCountCard
                            key={configs.type}
                            onClick={() => onSelectTemplate(configs.type)}
                            name={configs.displayName}
                            logoUrl={configs.logoUrl}
                            logoComponent={configs.logoComponent}
                        />
                    ))}
                </PlatformListContainer>
            </Section>
            <Button onClick={cancel}>Cancel</Button>
        </>
    );
};
