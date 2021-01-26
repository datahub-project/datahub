import { Button, Card, Divider, Pagination } from 'antd';
import * as React from 'react';

interface SearchResult {
    title?: React.ReactNode; // Title content
    preview?: React.ReactNode; // Body content
    onNavigate: () => void; // Invoked when the search result is clicked.
}

interface Props {
    typeName: string;
    pageStart: number;
    pageSize: number;
    totalResults: number;
    results: Array<SearchResult>;
    onChangePage: (page: number) => void;
}

export const SearchResults = ({
    typeName: _typeName,
    pageStart: _pageStart,
    pageSize: _pageSize,
    totalResults: _totalResults,
    results: _results,
    onChangePage,
}: Props) => {
    return (
        <Card
            style={{ border: '1px solid #d2d2d2' }}
            title={<h1 style={{ marginBottom: '0px' }}>{_typeName}</h1>}
            bodyStyle={{ padding: '24px 0px' }}
            extra={
                <div style={{ color: 'grey' }}>
                    Showing {_pageStart * _pageSize} - {_pageStart * _pageSize + _pageSize} of {_totalResults} results
                </div>
            }
        >
            {_results.map((result) => (
                <>
                    <div style={{ padding: '0px 24px' }}>
                        <Button onClick={result.onNavigate} style={{ color: '#0073b1', padding: '0px' }} type="link">
                            {result.title}
                        </Button>
                        <div style={{ padding: '20px 5px 5px 5px' }}>{result.preview}</div>
                    </div>
                    <Divider />
                </>
            ))}
            <Pagination
                style={{ width: '100%', display: 'flex', justifyContent: 'center' }}
                current={_pageStart}
                pageSize={_pageSize}
                total={_totalResults / _pageSize}
                showLessItems
                onChange={onChangePage}
            />
        </Card>
    );
};
