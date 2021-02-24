import { ArrowRightOutlined } from '@ant-design/icons';
import { Button, Card, Divider, List, Typography } from 'antd';
import * as React from 'react';
import { useHistory } from 'react-router-dom';
import { SearchCfg } from '../../conf';
import { useGetSearchResultsQuery } from '../../graphql/search.generated';
import { EntityType } from '../../types.generated';
import { IconStyleType } from '../entity/Entity';
import { useEntityRegistry } from '../useEntityRegistry';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';

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

    const results = data?.search?.entities || [];

    return (
        <>
            <List
                header={
                    <div style={{ marginBottom: 20 }}>
                        <Typography.Title level={3}>{entityRegistry.getCollectionName(type)}</Typography.Title>
                        <Card
                            bodyStyle={{ padding: '16px 24px' }}
                            style={{ right: '52px', top: '-40px', position: 'absolute' }}
                        >
                            {entityRegistry.getIcon(type, 36, IconStyleType.ACCENT)}
                        </Card>
                    </div>
                }
                footer={
                    data?.search &&
                    data?.search?.total > 0 && (
                        <Button
                            type="text"
                            style={{ fontSize: 18 }}
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
                style={{
                    width: '100%',
                    borderColor: '#f0f0f0',
                    marginTop: '24px',
                    marginBottom: 40,
                    padding: '16px 32px',
                }}
                dataSource={data?.search?.entities || []}
                split={false}
                renderItem={(item, index) => (
                    <>
                        <List.Item>{entityRegistry.renderSearchResult(type, item)}</List.Item>
                        {index < results.length - 1 && <Divider />}
                    </>
                )}
                bordered
            />
        </>
    );
};
