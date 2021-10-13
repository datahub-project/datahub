import { ArrowRightOutlined } from '@ant-design/icons';
import { Button, Card, Divider, List, Space, Typography } from 'antd';
import { ListProps } from 'antd/lib/list';
import * as React from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';
import { EntityType, SearchResult } from '../../types.generated';
import { IconStyleType } from '../entity/Entity';
import { useEntityRegistry } from '../useEntityRegistry';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import analytics, { EventType } from '../analytics';

const styles = {
    header: { marginBottom: 20 },
    resultHeaderCardBody: { padding: '16px 24px' },
    resultHeaderCard: { right: '52px', top: '-40px', position: 'absolute' },
    seeAllButton: { fontSize: 18 },
    resultsContainer: { width: '100%', padding: '40px 132px' },
};

const ResultList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        margin-top: 8px;
        padding: 16px 48px;
        box-shadow: ${(props) => props.theme.styles['box-shadow']};
    }
`;

interface Props {
    type: EntityType;
    query: string;
    searchResults: Array<SearchResult>;
}

export const EntityGroupSearchResults = ({ type, query, searchResults }: Props) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();

    const onResultClick = (result: SearchResult, index: number) => {
        analytics.event({
            type: EventType.SearchResultClickEvent,
            query,
            entityUrn: result.entity.urn,
            entityType: result.entity.type,
            index,
            total: searchResults.length,
        });
    };

    return (
        <Space direction="vertical" style={styles.resultsContainer}>
            <ResultList<React.FC<ListProps<SearchResult>>>
                header={
                    <span style={styles.header}>
                        <Typography.Title level={2}>{entityRegistry.getCollectionName(type)}</Typography.Title>
                        <Card bodyStyle={styles.resultHeaderCardBody} style={styles.resultHeaderCard as any}>
                            {entityRegistry.getIcon(type, 36, IconStyleType.ACCENT)}
                        </Card>
                    </span>
                }
                footer={
                    searchResults.length > 0 && (
                        <Button
                            type="text"
                            style={styles.seeAllButton}
                            onClick={() =>
                                navigateToSearchUrl({
                                    type,
                                    query,
                                    page: 0,
                                    history,
                                })
                            }
                        >
                            <Typography.Text>
                                See all <b>{entityRegistry.getCollectionName(type)}</b> results
                            </Typography.Text>
                            <ArrowRightOutlined />
                        </Button>
                    )
                }
                dataSource={searchResults as SearchResult[]}
                split={false}
                renderItem={(searchResult, index) => (
                    <>
                        <List.Item onClick={() => onResultClick(searchResult, index)}>
                            {entityRegistry.renderSearchResult(type, searchResult)}
                        </List.Item>
                        {index < searchResults.length - 1 && <Divider />}
                    </>
                )}
                bordered
            />
        </Space>
    );
};
