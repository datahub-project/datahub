import { BookOutlined, FormOutlined, PlusCircleOutlined, SearchOutlined } from '@ant-design/icons';
import { Button, Input, InputRef } from 'antd';
import React, { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import type { Capability } from '@app/ingestV2/shared/connectorRegistry';
import { useCapabilitySummary } from '@app/ingestV2/shared/hooks/useCapabilitySummary';
import { ConnectorDetailPopover } from '@app/ingestV2/source/builder/ConnectorDetailPopover';
import { DataPlatformCard } from '@app/ingestV2/source/builder/DataPlatformCard';
import { FILTER_OPTIONS, type SupportStatusFilter } from '@app/ingestV2/source/builder/SupportStatusBadge';
import { CUSTOM } from '@app/ingestV2/source/builder/constants';
import { IngestionSourceBuilderStep } from '@app/ingestV2/source/builder/steps';
import { SourceBuilderState, SourceConfig, StepProps } from '@app/ingestV2/source/builder/types';
import useGetSourceLogoUrl from '@app/ingestV2/source/builder/useGetSourceLogoUrl';

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

const ToolbarContainer = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 0 12px 12px 0;
    gap: 12px;
    flex-wrap: wrap;
`;

const FilterChipsContainer = styled.div`
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
`;

const FilterChip = styled.button<{ $active: boolean }>`
    display: inline-flex;
    align-items: center;
    padding: 4px 14px;
    border-radius: 16px;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    border: 1px solid ${(props) => (props.$active ? props.theme.colors.borderBrand : props.theme.colors.border)};
    background-color: ${(props) => (props.$active ? props.theme.colors.bgSurfaceBrand : props.theme.colors.bg)};
    color: ${(props) => (props.$active ? props.theme.colors.textBrand : props.theme.colors.textSecondary)};
    transition: all 0.2s;

    &:hover {
        border-color: ${(props) => props.theme.colors.borderBrand};
        color: ${(props) => props.theme.colors.textBrand};
    }
`;

const StyledSearchBar = styled(Input)`
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 8px;
    box-shadow: ${(props) => props.theme.colors.shadowSm};
    border: 1px solid ${(props) => props.theme.colors.border};
    max-width: 300px;
    font-size: 16px;
`;

const StyledSearchOutlined = styled(SearchOutlined)`
    color: ${(props) => props.theme.colors.textTertiary};
`;

const PlatformListContainer = styled.div`
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(min(100%, 31%), 1fr));
    gap: 10px;
    height: 100%;
    overflow-y: auto;
    padding-right: 12px;
    align-items: start;
`;

const CardWrapper = styled.div`
    height: 200px;
`;

const NoResultsContainer = styled.div`
    grid-column: 1 / -1;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    padding: 48px 20px;
    text-align: center;
    gap: 16px;
`;

const NoResultsTitle = styled.div`
    font-size: 16px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
`;

const NoResultsSubtitle = styled.div`
    font-size: 14px;
    color: ${(props) => props.theme.colors.textTertiary};
    max-width: 400px;
`;

const NoResultsActions = styled.div`
    display: flex;
    gap: 12px;
    margin-top: 8px;
`;

interface SourceOptionProps {
    source: SourceConfig;
    supportStatus?: string;
    capabilities?: Capability[];
    onClick: () => void;
}

function SourceOption({ source, supportStatus, capabilities, onClick }: SourceOptionProps) {
    const { name, displayName, description, docsUrl } = source;
    const theme = useTheme();

    const logoUrl = useGetSourceLogoUrl(name);
    let logoComponent;
    if (name === CUSTOM) {
        logoComponent = <FormOutlined style={{ color: theme.colors.textSecondary, fontSize: 28 }} />;
    }

    return (
        <ConnectorDetailPopover
            name={displayName}
            supportStatus={supportStatus}
            capabilities={capabilities}
            docsUrl={docsUrl}
        >
            <CardWrapper>
                <DataPlatformCard
                    onClick={onClick}
                    name={displayName}
                    logoUrl={logoUrl}
                    description={description}
                    logoComponent={logoComponent}
                    supportStatus={supportStatus}
                    dataTestId={`source-option-${name}`}
                />
            </CardWrapper>
        </ConnectorDetailPopover>
    );
}

/**
 * Component responsible for selecting the mechanism for constructing a new Ingestion Source
 */
export const SelectTemplateStep = ({
    state,
    updateState,
    goTo,
    ingestionSources,
    setSelectedSourceType,
}: StepProps) => {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const [searchFilter, setSearchFilter] = useState('');
    const [statusFilter, setStatusFilter] = useState<SupportStatusFilter>('ALL');
    const { getPluginCapabilities } = useCapabilitySummary();

    // Callback ref that focuses immediately when the element is attached
    const searchInputCallbackRef = (node: InputRef | null) => {
        if (node) {
            node.focus();
        }
    };

    const onSelectTemplate = (type: string) => {
        const newState: SourceBuilderState = {
            ...state,
            config: undefined,
            type,
        };
        updateState(newState);
        goTo(IngestionSourceBuilderStep.DEFINE_RECIPE);
        setSelectedSourceType?.(type);
    };

    // Build a map of source name → support status from the connector registry + source config
    const supportStatusMap = useMemo(() => {
        const map: Record<string, string> = {};
        ingestionSources.forEach((source) => {
            // Community plugins have supportStatus set directly on the SourceConfig
            if (source.supportStatus) {
                map[source.name] = source.supportStatus;
            } else {
                // Built-in plugins: look up from connector registry JSON
                const details = getPluginCapabilities(source.name);
                if (details?.support_status) {
                    map[source.name] = details.support_status;
                }
            }
        });
        return map;
    }, [ingestionSources, getPluginCapabilities]);

    const filteredSources = useMemo(() => {
        let sources = ingestionSources.filter(
            (source) =>
                source.displayName.toLocaleLowerCase().includes(searchFilter.toLocaleLowerCase()) ||
                source.name.toLocaleLowerCase().includes(searchFilter.toLocaleLowerCase()),
        );

        // Apply support status filter
        if (statusFilter !== 'ALL') {
            sources = sources.filter((source) => {
                const status = supportStatusMap[source.name];
                return status === statusFilter;
            });
        }

        sources.sort((a, b) => {
            if (a.name === 'custom') return 1;
            if (b.name === 'custom') return -1;
            return a.displayName.localeCompare(b.displayName);
        });

        return sources;
    }, [ingestionSources, searchFilter, statusFilter, supportStatusMap]);

    return (
        <Container>
            <Section>
                <ToolbarContainer>
                    <FilterChipsContainer>
                        {FILTER_OPTIONS.map((option) => (
                            <FilterChip
                                key={option.key}
                                $active={statusFilter === option.key}
                                onClick={() => setStatusFilter(option.key)}
                            >
                                {t(option.labelKey)}
                            </FilterChip>
                        ))}
                    </FilterChipsContainer>
                    <StyledSearchBar
                        ref={searchInputCallbackRef}
                        data-testid="source-type-search-input"
                        placeholder={t('selectTemplate.searchPlaceholder')}
                        value={searchFilter}
                        onChange={(e) => setSearchFilter(e.target.value)}
                        allowClear
                        prefix={<StyledSearchOutlined />}
                    />
                </ToolbarContainer>
                <PlatformListContainer data-testid="data-source-options">
                    {filteredSources.length > 0 ? (
                        filteredSources.map((source) => (
                            <SourceOption
                                key={source.urn}
                                source={source}
                                supportStatus={supportStatusMap[source.name]}
                                capabilities={getPluginCapabilities(source.name)?.capabilities}
                                onClick={() => onSelectTemplate(source.name)}
                            />
                        ))
                    ) : (
                        <NoResultsContainer>
                            <NoResultsTitle>
                                {searchFilter
                                    ? t('selectTemplate.noResults', { searchFilter })
                                    : t('selectTemplate.noResultsFiltered')}
                            </NoResultsTitle>
                            <NoResultsSubtitle>{t('selectTemplate.noResultsSubtitle')}</NoResultsSubtitle>
                            <NoResultsActions>
                                <Button
                                    type="primary"
                                    icon={<PlusCircleOutlined />}
                                    onClick={() => onSelectTemplate(CUSTOM)}
                                >
                                    {t('selectTemplate.useCustomSource')}
                                </Button>
                                <Button
                                    icon={<BookOutlined />}
                                    href="https://datahubproject.io/docs/metadata-ingestion/adding-source"
                                    target="_blank"
                                    rel="noopener noreferrer"
                                >
                                    {t('selectTemplate.buildConnector')}
                                </Button>
                            </NoResultsActions>
                        </NoResultsContainer>
                    )}
                </PlatformListContainer>
            </Section>
        </Container>
    );
};
