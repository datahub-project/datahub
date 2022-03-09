import React from 'react';
import styled from 'styled-components';
import { Alert, Col, Row, PageHeader, Space, Typography, Divider, Dropdown, Button, Menu } from 'antd';
import { RightOutlined, FolderOutlined, EllipsisOutlined, FolderAddOutlined, PlusOutlined } from '@ant-design/icons';
import { Link } from 'react-router-dom';
import * as QueryString from 'query-string';
import { useLocation } from 'react-router';
import { SearchablePage } from '../search/SearchablePage';
import { useGetBrowseResultsQuery } from '../../graphql/browse.generated';
import { BrowseCfg } from '../../conf';
import { EntityType, GlossaryTerm } from '../../types.generated';
import { GlossariesTreeSidebar } from './GlossariesTreeSidebar';

const ManageGlossariesHeader = styled(PageHeader)`
    box-shadow: 0px 2px 6px rgba(0, 0, 0, 0.05);
`;

const styles = {
    row: { padding: 20, marginTop: 10, paddingRight: 30 },
    divider: { margin: 6 },
    folder: { fontSize: 18 },
    menuItem: {
        padding: '10px 20px',
    },
};

const ResultLink = styled(Link)`
    color: #262626;

    &&:hover {
        color: #262626;
    }
`;

const menu = (
    <Menu>
        <Menu.Item style={styles.menuItem}>
            <a target="_blank" rel="noopener noreferrer" href="http://www.alipay.com/">
                <Space align="baseline">
                    <PlusOutlined />
                    Add Term
                </Space>
            </a>
        </Menu.Item>
        <Menu.Item style={styles.menuItem}>
            <a target="_blank" rel="noopener noreferrer" href="http://www.taobao.com/">
                <Space align="baseline">
                    <FolderAddOutlined />
                    Add Node
                </Space>
            </a>
        </Menu.Item>
    </Menu>
);

const ResultCard = ({ to, name, count }: { to: string; name?: string; count?: number }) => (
    <ResultLink to={to}>
        <Row key={`${name}_key`} justify="space-between" style={styles.row}>
            <Space size="middle" align="baseline">
                <FolderOutlined style={styles.folder} />
                <Typography.Title level={5}>{name}</Typography.Title>
                {count && <Typography.Text strong>{count}</Typography.Text>}
            </Space>
            {count && (
                <Space>
                    <RightOutlined />
                </Space>
            )}
        </Row>
        <Divider style={styles.divider} />
    </ResultLink>
);

export const ManageGlossaries = () => {
    const location = useLocation();
    const params = QueryString.parse(location.search);

    const rootPath = location.pathname;
    const path = rootPath.split('/').slice(2);

    const page: number = params.page && Number(params.page as string) > 0 ? Number(params.page as string) : 1;

    const entityType = EntityType.GlossaryTerm;

    const { data, loading, error } = useGetBrowseResultsQuery({
        variables: {
            input: {
                type: entityType,
                path,
                start: (page - 1) * BrowseCfg.RESULTS_PER_PAGE,
                count: BrowseCfg.RESULTS_PER_PAGE,
                filters: null,
            },
        },
    });

    if (error || (!loading && !error && !data)) {
        return <Alert type="error" message={error?.message || 'Entity failed to load'} />;
    }

    const DropdownMenu = () => (
        <Dropdown key="more" overlay={menu}>
            <Button
                style={{
                    border: 'none',
                    padding: 0,
                    boxShadow: 'none',
                    transform: 'rotate(90deg)',
                }}
            >
                <EllipsisOutlined
                    style={{
                        fontSize: 25,
                        verticalAlign: 'top',
                    }}
                />
            </Button>
        </Dropdown>
    );

    return (
        <SearchablePage>
            <Row>
                <Col
                    span={4}
                    style={{
                        borderRight: '1px solid #E9E9E9',
                        padding: 40,
                    }}
                >
                    <GlossariesTreeSidebar />
                </Col>
                <Col span={20}>
                    <ManageGlossariesHeader title="Glossary Terms" extra={[<DropdownMenu key="more" />]} />
                    {data && data.browse && (
                        <>
                            {data.browse.groups.map(({ name, count }) => (
                                <ResultCard
                                    to={`${rootPath}/${name}`}
                                    name={name}
                                    count={count}
                                    key={`groups-${name}`}
                                />
                            ))}

                            {(!(data.browse.groups && data.browse.groups.length > 0) ||
                                (data.browse.entities && data.browse.entities.length > 0)) &&
                                (data?.browse?.entities as GlossaryTerm[]).map(({ properties, urn }) => (
                                    <ResultCard
                                        to={`${rootPath}/${urn}`}
                                        name={properties?.name}
                                        key={`entities-${properties?.name}`}
                                    />
                                ))}
                        </>
                    )}
                </Col>
            </Row>
        </SearchablePage>
    );
};
