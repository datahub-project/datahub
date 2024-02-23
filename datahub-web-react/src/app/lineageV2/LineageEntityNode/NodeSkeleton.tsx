import React from 'react';
import { Skeleton } from 'antd';
import styled from 'styled-components';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
    width: 100%;

    &&& {
        ul {
            margin: 0;
        }

        li {
            height: 1em;
            margin-top: 4px;
        }
    }
`;

export default function NodeSkeleton() {
    return (
        <Wrapper>
            <Skeleton active title={{ width: '100%' }} paragraph={{ rows: 2 }} />
        </Wrapper>
    );
}
