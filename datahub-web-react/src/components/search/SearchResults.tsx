import { Card, Pagination } from 'antd';
import * as React from 'react';

interface Props {
    typeName: string;
    pageStart: number;
    pageSize: number;
    totalResults: number;
    results: Array<JSX.Element>;
    onChangePage: (page: number) => void;
}

export const SearchResults = ({ typeName, pageStart, pageSize, totalResults, results, onChangePage }: Props) => {
    return (
        <Card
            style={{ border: '1px solid #d2d2d2' }}
            title={<h1 style={{ marginBottom: '0px' }}>{typeName}</h1>}
            bodyStyle={{ padding: '24px 0px' }}
            extra={
                <div style={{ color: 'grey' }}>
                    Showing {pageStart * pageSize} - {pageStart * pageSize + pageSize} of {totalResults} results
                </div>
            }
        >
            {results}
            <Pagination
                style={{ width: '100%', display: 'flex', justifyContent: 'center' }}
                current={pageStart}
                pageSize={pageSize}
                total={totalResults / pageSize}
                showLessItems
                onChange={onChangePage}
            />
        </Card>
    );
};
