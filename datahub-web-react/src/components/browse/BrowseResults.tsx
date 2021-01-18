import { Button, Col, Pagination, Row } from 'antd';
import { Content } from 'antd/lib/layout/layout';
import * as React from 'react';

export interface BrowseResult {
    name: string; // Browse Path content
    count?: number; // Number of children results
    onNavigate: () => void; // Invoked when the browse result is clicked.
}

interface Props {
    title: string;
    pageStart: number;
    pageSize: number;
    totalResults: number;
    results: Array<BrowseResult>;
    onChangePage: (page: number) => void;
}

/**
 * Display browse groups + entities.
 */
export const BrowseResults = ({
    title: _title,
    pageStart: _pageStart,
    pageSize: _pageSize,
    totalResults: _totalResults,
    results: _results,
    onChangePage: _onChangePage,
}: Props) => {
    return (
        <div>
            <Content>
                <Row style={{ backgroundColor: 'white', padding: '25px 100px' }}>
                    <Col span={24}>
                        <h1 style={{ fontSize: '25px' }}>{_title}</h1>
                        <ul style={{ padding: '0px' }}>
                            {_results.map((result) => (
                                <li
                                    style={{
                                        margin: '10px 0px',
                                        display: 'flex',
                                        width: '100%',
                                        height: '50px',
                                    }}
                                >
                                    <Button
                                        style={{
                                            border: '1px solid #d2d2d2',
                                            borderRadius: '5px',
                                            width: '100%',
                                            height: '100%',
                                            display: 'flex',
                                            justifyContent: 'space-between',
                                            alignItems: 'center',
                                        }}
                                        onClick={() => result.onNavigate()}
                                    >
                                        <div style={{ fontSize: '12px', fontWeight: 'bold', color: '#0073b1' }}>
                                            {result.name}
                                        </div>
                                        <div>{result.count && `${result.count}}`}</div>
                                    </Button>
                                </li>
                            ))}
                        </ul>
                        <Pagination
                            style={{ width: '100%', display: 'flex', justifyContent: 'center' }}
                            current={_pageStart}
                            pageSize={_pageSize}
                            total={_totalResults / _pageSize}
                            showLessItems
                            onChange={_onChangePage}
                        />
                    </Col>
                </Row>
            </Content>
        </div>
    );
};
