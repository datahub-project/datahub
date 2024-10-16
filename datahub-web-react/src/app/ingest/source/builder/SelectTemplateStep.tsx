import React, { useState } from 'react';

import { Button, Input } from 'antd';
import { FormOutlined, SearchOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { SourceConfig, SourceBuilderState, StepProps } from './types';
import { IngestionSourceBuilderStep } from './steps';
import useGetSourceLogoUrl from './useGetSourceLogoUrl';
import { CUSTOM } from './constants';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { DataPlatformCard } from './DataPlatformCard';

const Container = styled.div`
    max-height: 82vh;
    display: flex;
    flex-direction: column;
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    padding-bottom: 12px;
    overflow: hidden;
`;

const CancelButton = styled(Button)`
    max-width: 120px;
`;

const SearchBarContainer = styled.div`
    display: flex;
    justify-content: end;
    width: auto;
    padding-right: 12px;
`;

const StyledSearchBar = styled(Input)`
    background-color: white;
    border-radius: 8px;
    box-shadow: 0px 0px 30px 0px rgb(239 239 239);
    border: 1px solid #e0e0e0;
    margin: 0 0 15px 0px;
    max-width: 300px;
    font-size: 16px;
`;

const StyledSearchOutlined = styled(SearchOutlined)`
    color: #a9adbd;
`;

const PlatformListContainer = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(min(100%, 31%), 1fr));
    gap: 10px;
    height: 100%;
    overflow-y: auto;
    padding-right: 12px;
`;

interface SourceOptionProps {
    source: SourceConfig;
    onClick: () => void;
}

function SourceOption({ source, onClick }: SourceOptionProps) {
    const { name, displayName, description } = source;

    const logoUrl = useGetSourceLogoUrl(name);
    let logoComponent;
    if (name === CUSTOM) {
        logoComponent = <FormOutlined style={{ color: ANTD_GRAY[8], fontSize: 28 }} />;
    }

    return (
        <DataPlatformCard
            onClick={onClick}
            name={displayName}
            logoUrl={logoUrl}
            description={description}
            logoComponent={logoComponent}
        />
    );
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
        (source) =>
            source.displayName.toLocaleLowerCase().includes(searchFilter.toLocaleLowerCase()) ||
            source.name.toLocaleLowerCase().includes(searchFilter.toLocaleLowerCase()),
    );

    filteredSources.sort((a, b) => {
        if (a.name === 'custom') {
            return 1;
        }

        if (b.name === 'custom') {
            return -1;
        }

        return a.displayName.localeCompare(b.displayName);
    });

    return (
        <Container>
            <Section>
                <SearchBarContainer>
                    <StyledSearchBar
                        placeholder="Search data sources..."
                        value={searchFilter}
                        onChange={(e) => setSearchFilter(e.target.value)}
                        allowClear
                        prefix={<StyledSearchOutlined />}
                    />
                </SearchBarContainer>
                <PlatformListContainer data-testid="data-source-options">
                    {filteredSources.map((source) => (
                        <SourceOption key={source.urn} source={source} onClick={() => onSelectTemplate(source.name)} />
                    ))}
                </PlatformListContainer>
            </Section>
            <CancelButton onClick={cancel}>Cancel</CancelButton>
        </Container>
    );
};
