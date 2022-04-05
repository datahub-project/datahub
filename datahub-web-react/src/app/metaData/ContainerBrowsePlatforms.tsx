import React from 'react';
import { Content } from 'antd/lib/layout/layout';
import { Row, Col, Pagination, Alert, Space, Typography, Card } from 'antd';
import { useHistory, useLocation } from 'react-router';
import { Link } from 'react-router-dom';
import { RightOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import * as QueryString from 'query-string';

import { BrowseCfg } from '../../conf';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { DataPlatform, EntityType } from '../../types.generated';
import { PLATFORM_FILTER_NAME } from '../search/utils/constants';
import { Message } from '../shared/Message';
import { formatNumber } from '../shared/formatNumber';

const styles = {
    row: { padding: '8px 8px 8px 76px' },
    title: { margin: 0 },
    type: { fontSize: '12px', lineHeight: '20px', fontWeight: 700, color: '#8C8C8C' },
    count: { margin: 0, color: '#00000073' },
};

const ResultContainer = styled(Content)`
    min-height: calc(100vh - 110px);
`;

const PreviewImage = styled.img`
    max-height: 18px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
    margin-right: 4px;
`;

const PaginationInfoContainer = styled(Row)`
    position: absolute;
    bottom: 0;
    width: 100%;
    height: 60px;
    border: 1px solid ${(props) => props.theme.styles['border-color-base']};
`;

const ResultCard = styled(Card)`
    && {
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }
    &&:hover {
        box-shadow: ${(props) => props.theme.styles['box-shadow-hover']};
    }
`;

interface Props {
    rootPath: string;
    entityType: EntityType;
}

export const ContainerBrowsePlatforms = ({ rootPath, entityType }: Props) => {
    const history = useHistory();
    const location = useLocation();
    const params = QueryString.parse(location.search);
    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;

    // Fetching the Meta Data Results
    const { data, loading, error } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [entityType],
                query: '*',
                start: (page - 1) * BrowseCfg.RESULTS_PER_PAGE,
                count: BrowseCfg.RESULTS_PER_PAGE,
                filters: [],
            },
        },
    });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const platforms = data?.searchAcrossEntities?.facets?.filter((facet) => {
        return facet.field === PLATFORM_FILTER_NAME;
    });

    const onChangePage = (newPage: number) => {
        history.push({
            pathname: rootPath,
            search: `&page=${newPage}`,
        });
    };

    return (
        <div>
            {loading && <Message type="loading" content="Loading..." style={{ marginTop: '10%' }} />}
            <ResultContainer>
                <Row>
                    {platforms &&
                        platforms[0]?.aggregations.map((aggregation) => {
                            const platform = aggregation.entity as DataPlatform;
                            const displayName =
                                platform.properties?.displayName || platform.info?.displayName || platform.name;
                            return (
                                <>
                                    <Col span={24} key={`${displayName}_key`}>
                                        <Link to={`${rootPath}/${platform.urn}`}>
                                            <ResultCard>
                                                <Row style={styles.row} justify="space-between">
                                                    <Space size="middle" align="center">
                                                        {!!platform.properties?.logoUrl && (
                                                            <PreviewImage
                                                                src={platform.properties?.logoUrl}
                                                                alt={displayName}
                                                            />
                                                        )}
                                                        <Typography.Title style={styles.title} level={5}>
                                                            {displayName}
                                                        </Typography.Title>

                                                        <Typography.Title style={styles.count} level={5}>
                                                            {formatNumber(aggregation.count)}
                                                        </Typography.Title>
                                                    </Space>
                                                    <Space size="middle" align="center">
                                                        <RightOutlined />
                                                    </Space>
                                                </Row>
                                            </ResultCard>
                                        </Link>
                                    </Col>
                                </>
                            );
                        })}
                </Row>
                <PaginationInfoContainer>
                    <Col span={24}>
                        <Pagination
                            style={{ width: '100%', display: 'flex', justifyContent: 'center', paddingTop: 16 }}
                            current={page}
                            pageSize={BrowseCfg.RESULTS_PER_PAGE}
                            total={platforms?.length}
                            showTitle
                            showLessItems
                            onChange={onChangePage}
                            showSizeChanger={false}
                        />
                    </Col>
                </PaginationInfoContainer>
            </ResultContainer>
        </div>
    );
};
