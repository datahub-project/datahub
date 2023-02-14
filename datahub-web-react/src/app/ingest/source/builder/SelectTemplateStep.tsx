import { Button, Input } from 'antd';
import { FormOutlined, SearchOutlined } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';
import { LogoCountCard } from '../../../shared/LogoCountCard';
import { SourceConfig, SourceBuilderState, StepProps } from './types';
import { IngestionSourceBuilderStep } from './steps';
import useGetSourceLogoUrl from './useGetSourceLogoUrl';
import { CUSTOM } from './constants';
import { ANTD_GRAY } from '../../../entity/shared/constants';

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

const StyledSearchBar = styled(Input)`
    background-color: white;
    border-radius: 70px;
    box-shadow: 0px 0px 30px 0px rgb(239 239 239);
    width: 45%;
    margin: 0 0 15px 12px;
`;

interface SourceOptionProps {
    source: SourceConfig;
    onClick: () => void;
}

function SourceOption({ source, onClick }: SourceOptionProps) {
    const { name, displayName } = source;

    const logoUrl = useGetSourceLogoUrl(name);
    let logoComponent;
    if (name === CUSTOM) {
        logoComponent = <FormOutlined style={{ color: ANTD_GRAY[8], fontSize: 28 }} />;
    }

    return <LogoCountCard onClick={onClick} name={displayName} logoUrl={logoUrl} logoComponent={logoComponent} />;
}

/**
 * Component responsible for selecting the mechanism for constructing a new Ingestion Source
 */
export const SelectTemplateStep = ({ state, updateState, goTo, cancel, ingestionSources }: StepProps) => {
    const [searchFilter, setSearchFilter] = useState('');

    const onSelectTemplate = (type: string) => {
        const newState: SourceBuilderState = {
            ...state,
            config: undefined,
            type,
        };
        updateState(newState);
        goTo(IngestionSourceBuilderStep.DEFINE_RECIPE);
    };

    const filteredSources = ingestionSources.filter(
        (source) => source.displayName.includes(searchFilter) || source.name.includes(searchFilter),
    );

    return (
        <>
            <Section>
                <StyledSearchBar
                    placeholder="Search ingestion sources..."
                    value={searchFilter}
                    onChange={(e) => setSearchFilter(e.target.value)}
                    allowClear
                    prefix={<SearchOutlined />}
                />
                <PlatformListContainer>
                    {filteredSources.map((source) => (
                        <SourceOption key={source.urn} source={source} onClick={() => onSelectTemplate(source.name)} />
                    ))}
                </PlatformListContainer>
            </Section>
            <CancelButton onClick={cancel}>Cancel</CancelButton>
        </>
    );
};
