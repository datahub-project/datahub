import { Result } from 'antd';
import React from 'react';
import { SearchablePage } from '../search/SearchablePage';

export const UnauthorizedPage = () => {
    return (
        <SearchablePage>
            <Result status="403" title="Unauthorized" subTitle="Sorry, you are not authorized to access this page." />
        </SearchablePage>
    );
};
