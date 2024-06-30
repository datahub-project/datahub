import React from 'react';
import { Skeleton } from 'antd';
import styled from 'styled-components';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    height: 100%;
    width: 100%;

    &&& {
        ul {
            margin: 0;
        }

        li {
            height: 1em;
            :not(:first-child) {
                margin-top: 4px;
            }
        }
    }
`;

interface Props {
    className?: string;
}

export default function NodeSkeleton({ className }: Props) {
    return (
        <Wrapper className={className}>
            <Skeleton active title={false} paragraph={{ rows: 3 }} />
        </Wrapper>
    );
}
