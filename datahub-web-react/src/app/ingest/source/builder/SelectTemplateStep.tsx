import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { LogoCountCard } from '../../../shared/LogoCountCard';
import { SourceBuilderState, StepProps } from './types';
import { SOURCE_TEMPLATE_CONFIGS } from '../conf/sources';
import { IngestionSourceBuilderStep } from './steps';

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

const CancelButton = styled(Button)`
    && {
        margin-left: 12px;
    }
`;

/**
 * Component responsible for selecting the mechanism for constructing a new Ingestion Source
 */
export const SelectTemplateStep = ({ state, updateState, goTo, cancel }: StepProps) => {
    // Reoslve the supported platform types to their logos and names.

    const onSelectTemplate = (type: string) => {
        const newState: SourceBuilderState = {
            ...state,
            config: undefined,
            type,
        };
        updateState(newState);
        goTo(IngestionSourceBuilderStep.DEFINE_RECIPE);
    };

    return (
        <>
            <Section>
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
            <CancelButton onClick={cancel}>Cancel</CancelButton>
        </>
    );
};
