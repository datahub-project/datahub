import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { LogoCountCard } from '../../../shared/LogoCountCard';
import { SourceConfig, SourceBuilderState, StepProps } from './types';
import { IngestionSourceBuilderStep } from './steps';
import useGetSourceLogoUrl from './useGetSourceLogoUrl';

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

interface SourceOptionProps {
    source: SourceConfig;
    onClick: () => void;
}

function SourceOption({ source, onClick }: SourceOptionProps) {
    const { urn, displayName } = source;

    const logoUrl = useGetSourceLogoUrl(urn);

    return <LogoCountCard onClick={onClick} name={displayName} logoUrl={logoUrl} />;
}

/**
 * Component responsible for selecting the mechanism for constructing a new Ingestion Source
 */
export const SelectTemplateStep = ({ state, updateState, goTo, cancel, ingestionSources }: StepProps) => {
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
                    {ingestionSources.map((source) => (
                        <SourceOption key={source.urn} source={source} onClick={() => onSelectTemplate(source.name)} />
                    ))}
                </PlatformListContainer>
            </Section>
            <CancelButton onClick={cancel}>Cancel</CancelButton>
        </>
    );
};
