/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Skeleton } from 'antd';
import * as React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

const Container = styled.div`
    width: 100%;
    display: flex;
    flex-direction: column;
    padding: 24px 0px 20px 40px;
    overflow: auto;
    flex: 1;
`;

const cardStyle = {
    backgroundColor: ANTD_GRAY[2],
    height: 120,
    minWidth: '98%',
    borderRadius: 8,
    marginBottom: 20,
};

export default function SearchResultsLoadingSection() {
    return (
        <Container>
            <Skeleton.Input active style={cardStyle} />
            <Skeleton.Input active style={cardStyle} />
            <Skeleton.Input active style={cardStyle} />
            <Skeleton.Input active style={cardStyle} />
            <Skeleton.Input active style={cardStyle} />
        </Container>
    );
}
