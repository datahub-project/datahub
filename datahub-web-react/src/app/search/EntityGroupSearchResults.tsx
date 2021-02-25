import { ArrowRightOutlined } from '@ant-design/icons';
import { Button, Card, Divider, List, Space, Typography } from 'antd';
import * as React from 'react';
import { useHistory } from 'react-router-dom';
import { SearchCfg } from '../../conf';
import { useGetSearchResultsQuery } from '../../graphql/search.generated';
import { EntityType } from '../../types.generated';
import { IconStyleType } from '../entity/Entity';
import { useEntityRegistry } from '../useEntityRegistry';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';

const styles = {
    header: { marginBottom: 20 },
    resultHeaderCardBody: { padding: '16px 24px' },
    resultHeaderCard: { right: '52px', top: '-40px', position: 'absolute' },
    resultList: { width: '100%', borderColor: '#f0f0f0', marginTop: '12px', padding: '16px 32px' },
    seeAllButton: { fontSize: 18 },
    resultsContainer: { width: '100%', padding: '40px 132px' },
};

interface Props {
    type: EntityType;
    query: string;
}

export const EntityGroupSearchResults = ({ type, query }: Props) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { data } = useGetSearchResultsQuery({
        variables: {
            input: {
                type,
                query,
                start: 0,
                count: SearchCfg.RESULTS_PER_PAGE,
                filters: null,
            },
        },
    });

    if (!data?.search?.entities.length) {
        return null;
    }

    const results = data?.search?.entities || [];

    return (
        <Space direction="vertical" style={styles.resultsContainer}>
            <List
                header={
                    <span style={styles.header}>
                        <Typography.Title level={3}>{entityRegistry.getCollectionName(type)}</Typography.Title>
                        <Card bodyStyle={styles.resultHeaderCardBody} style={styles.resultHeaderCard as any}>
                            {entityRegistry.getIcon(type, 36, IconStyleType.ACCENT)}
                        </Card>
                    </span>
                }
                footer={
                    data?.search &&
                    data?.search?.total > 0 && (
                        <Button
                            type="text"
                            style={styles.seeAllButton}
                            onClick={() =>
                                navigateToSearchUrl({
                                    type,
                                    query,
                                    page: 0,
                                    history,
                                    entityRegistry,
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
                style={styles.resultList}
                dataSource={results}
                split={false}
                renderItem={(item, index) => (
                    <>
                        <List.Item>{entityRegistry.renderSearchResult(type, item)}</List.Item>
                        {index < results.length - 1 && <Divider />}
                    </>
                )}
                bordered
            />
        </Space>
    );
};
