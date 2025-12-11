/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Skeleton } from 'antd';
import React from 'react';
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
    numRows?: number;
    className?: string;
}

export default function NodeSkeleton({ numRows = 3, className }: Props) {
    return (
        <Wrapper className={className}>
            <Skeleton active title={false} paragraph={{ rows: numRows }} />
        </Wrapper>
    );
}
