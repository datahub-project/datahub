import { Result } from 'antd';
import React from 'react';

export const UnauthorizedPage = () => {
    return (
        <>
            <Result status="403" title="Unauthorized" subTitle="Sorry, you are not authorized to access this page." />
        </>
    );
};
