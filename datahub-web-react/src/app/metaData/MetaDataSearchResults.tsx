import React from 'react';
import { Card, Col, Pagination, Row, Space, Typography } from 'antd';
import styled from 'styled-components';
import { Content } from 'antd/lib/layout/layout';
import { Link } from 'react-router-dom';
import { InfoCircleOutlined, RightOutlined } from '@ant-design/icons';

import {
    FacetMetadata,
    FacetFilterInput,
    SearchResults as SearchResultType,
    Container,
    Entity,
    EntityType,
} from '../../types.generated';
import { SearchFilters } from '../search/SearchFilters';
import { SearchCfg } from '../../conf';
import { ReactComponent as LoadingSvg } from '../../images/datahub-logo-color-loading_pendulum.svg';
import EntityRegistry from '../entity/EntityRegistry';
import { CONTAINER_FILTER_NAME, DATABASE_FILTER_NAME, SCHEMA_FILTER_NAME } from '../search/utils/constants';
import analytics, { EventType } from '../analytics';

const SearchBody = styled.div`
    display: flex;
    flex-direction: row;
`;

const PaginationInfo = styled(Typography.Text)`
    padding: 0px;
`;

const FiltersContainer = styled.div`
    display: block;
    max-width: 260px;
    min-width: 260px;
    border-right: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
`;

const ResultContainer = styled.div`
    flex: 1;
    margin-bottom: 20px;
`;

const PaginationInfoContainer = styled.div`
    padding: 8px;
    padding-left: 16px;
    border-bottom: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const FiltersHeader = styled.div`
    font-size: 14px;
    font-weight: 600;

    padding-left: 20px;
    padding-right: 20px;
    padding-bottom: 8px;

    width: 100%;
    height: 46px;
    line-height: 46px;
    border-bottom: 1px solid;
    border-color: ${(props) => props.theme.styles['border-color-base']};
`;

const StyledPagination = styled(Pagination)`
    margin: 0px;
    padding: 0px;
`;

const SearchFilterContainer = styled.div`
    padding-top: 10px;
`;

const LoadingText = styled.div`
    margin-top: 18px;
    font-size: 12px;
`;

const LoadingContainer = styled.div`
    padding-top: 40px;
    padding-bottom: 40px;
    width: 100%;
    text-align: center;
`;

const EntityData = styled.div`
    && {
        width: 100%;
        margin-top: 12px;
        padding: 16px 32px;
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }
`;

const styles = {
    row: { padding: 8 },
    title: { margin: 0 },
    type: { fontSize: '12px', lineHeight: '20px', fontWeight: 700, color: '#8C8C8C' },
    count: { margin: 0, color: '#00000073' },
};

const PreviewImage = styled.img`
    max-height: 18px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
    margin-right: 4px;
`;

const ResultCard = styled(Card)`
    && {
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }
    &&:hover {
        box-shadow: ${(props) => props.theme.styles['box-shadow-hover']};
    }
`;

const ContainerDetailLink = styled(Link)`
    display: inline-block;
`;

interface Props {
    page: number;
    searchResponse?: SearchResultType | null;
    filters?: Array<FacetMetadata> | null;
    selectedFilters: Array<FacetFilterInput>;
    loading: boolean;
    showFilters?: boolean;
    onChangeFilters: (filters: Array<FacetFilterInput>) => void;
    onChangePage: (page: number) => void;
    entityRegistry: EntityRegistry;
    entityType: EntityType;
    rootPath: string;
}

export const MetaDataSearchResults = ({
    page,
    searchResponse,
    filters,
    selectedFilters,
    loading,
    showFilters,
    onChangeFilters,
    onChangePage,
    entityRegistry,
    entityType,
    rootPath,
}: Props) => {
    const pageStart = searchResponse?.start || 0;
    const pageSize = searchResponse?.count || 0;
    const totalResults = searchResponse?.total || 0;
    const lastResultIndex = pageStart + pageSize > totalResults ? totalResults : pageStart + pageSize;

    const onFilterSelect = (newFilters) => {
        onChangeFilters(newFilters);
    };

    const onEntityClick = (entity: Entity) => {
        analytics.event({
            type: EventType.ContainerBrowseClickEvent,
            path: rootPath,
            entityType,
            resultType: 'Entity',
            entityUrn: entity.urn,
        });
    };

    return (
        <>
            <SearchBody>
                {!!showFilters && (
                    <FiltersContainer>
                        <FiltersHeader>Filter</FiltersHeader>
                        <SearchFilterContainer>
                            <SearchFilters
                                loading={loading}
                                facets={filters || []}
                                selectedFilters={selectedFilters}
                                onFilterSelect={onFilterSelect}
                            />
                        </SearchFilterContainer>
                    </FiltersContainer>
                )}
                <ResultContainer>
                    {loading && (
                        <LoadingContainer>
                            <LoadingSvg height={80} width={80} />
                            <LoadingText>Searching for related entities...</LoadingText>
                        </LoadingContainer>
                    )}
                    {!loading && searchResponse?.searchResults?.length && (
                        <Content>
                            <Row>
                                {searchResponse?.searchResults.map((searchResult) => {
                                    let ui;
                                    if (
                                        searchResult.entity.type ===
                                        entityRegistry.getTypeFromPathName(CONTAINER_FILTER_NAME)
                                    ) {
                                        const containerEntity = searchResult.entity as Container;
                                        const type = containerEntity?.subTypes?.typeNames?.[0];
                                        const displayName = entityRegistry.getDisplayName(entityType, containerEntity);
                                        ui = (
                                            <>
                                                <Col span={24} key={`${displayName}_key`}>
                                                    <Link to={`${rootPath}/${containerEntity.urn}`}>
                                                        <ResultCard>
                                                            <Row style={styles.row} justify="space-between">
                                                                <Space size="middle" align="center">
                                                                    {!!containerEntity.platform.properties?.logoUrl && (
                                                                        <PreviewImage
                                                                            src={
                                                                                containerEntity.platform.properties
                                                                                    ?.logoUrl
                                                                            }
                                                                            alt={displayName}
                                                                        />
                                                                    )}
                                                                    <span>
                                                                        <span style={styles.type}>{type}</span>
                                                                        <Typography.Title
                                                                            style={styles.title}
                                                                            level={5}
                                                                        >
                                                                            {displayName}
                                                                        </Typography.Title>
                                                                    </span>
                                                                    {searchResponse?.searchResults.length && (
                                                                        <Typography.Title
                                                                            style={styles.count}
                                                                            level={5}
                                                                        >
                                                                            {searchResponse?.searchResults.length}
                                                                        </Typography.Title>
                                                                    )}
                                                                </Space>
                                                                <Space size="middle" align="center">
                                                                    {(type === DATABASE_FILTER_NAME ||
                                                                        type === SCHEMA_FILTER_NAME) && (
                                                                        <ContainerDetailLink
                                                                            to={entityRegistry.getEntityUrl(
                                                                                EntityType.Container,
                                                                                containerEntity.urn,
                                                                            )}
                                                                        >
                                                                            <Typography.Title
                                                                                style={styles.title}
                                                                                level={5}
                                                                            >
                                                                                <InfoCircleOutlined /> Details
                                                                            </Typography.Title>
                                                                        </ContainerDetailLink>
                                                                    )}

                                                                    <RightOutlined />
                                                                </Space>
                                                            </Row>
                                                        </ResultCard>
                                                    </Link>
                                                </Col>
                                            </>
                                        );
                                    } else {
                                        const { entity } = searchResult;
                                        const displayName = entityRegistry.getDisplayName(entityType, entity);
                                        ui = (
                                            <>
                                                <Col span={24} key={`${displayName}_key`}>
                                                    <EntityData onClick={() => onEntityClick(entity as Entity)}>
                                                        {entityRegistry.renderBrowse(entityType, entity)}
                                                    </EntityData>
                                                </Col>
                                            </>
                                        );
                                    }
                                    return ui;
                                })}
                            </Row>
                        </Content>
                    )}
                    <PaginationInfoContainer>
                        <PaginationInfo>
                            <b>
                                {lastResultIndex > 0 ? (page - 1) * pageSize + 1 : 0} - {lastResultIndex}
                            </b>{' '}
                            of <b>{totalResults}</b>
                        </PaginationInfo>
                        <StyledPagination
                            current={page}
                            pageSize={SearchCfg.RESULTS_PER_PAGE}
                            total={totalResults}
                            showLessItems
                            onChange={onChangePage}
                            showSizeChanger={false}
                        />
                        <span />
                    </PaginationInfoContainer>
                </ResultContainer>
            </SearchBody>
        </>
    );
};
