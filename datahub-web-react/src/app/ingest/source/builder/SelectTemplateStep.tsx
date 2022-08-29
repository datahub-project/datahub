import { Button } from 'antd';
import React, { useEffect } from 'react';
import styled from 'styled-components';
import { LogoCountCard } from '../../../shared/LogoCountCard';
import { IngestionSource, SourceBuilderState, StepProps } from './types';
import { SOURCE_TEMPLATE_CONFIGS } from '../conf/sources';
import { IngestionSourceBuilderStep } from './steps';
import { useGetDataPlatformLazyQuery } from '../../../../graphql/dataPlatform.generated';
import { SOURCE_TO_LOGO } from './constants';

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
    source: IngestionSource;
    onClick: () => void;
}

function SourceOption({ source, onClick }: SourceOptionProps) {
    const { urn, name } = source;

    const [getDataPlatform, { data, loading }] = useGetDataPlatformLazyQuery();
    const logoInMemory = SOURCE_TO_LOGO[urn];

    useEffect(() => {
        if (!logoInMemory && !data && !loading) {
            getDataPlatform({ variables: { urn } });
        }
    });

    return (
        <LogoCountCard
            onClick={onClick}
            name={name}
            logoUrl={logoInMemory || data?.dataPlatform?.properties?.logoUrl}
        />
    );
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
