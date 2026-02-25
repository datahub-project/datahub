import { Loader } from '@components';
import React from 'react';
import styled from 'styled-components';

const LoaderWrapper = styled.div`
    padding: 4px;
`;

export default function MenuLoader() {
    return (
        <LoaderWrapper>
            <Loader size="sm" />
        </LoaderWrapper>
    );
}
