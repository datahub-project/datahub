/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { LoadingOutlined } from '@ant-design/icons';
import { Spin } from 'antd';
import * as React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

const Container = styled.div`
    height: auto;
    width: 100%;
    display: flex;
    justify-content: center;
    align-items: center;
    background-color: rgb(250, 250, 250);
`;

const StyledLoading = styled(LoadingOutlined)`
    font-size: 32px;
    color: ${ANTD_GRAY[7]};
    padding-bottom: 18px;
]`;

export default function LineageLoadingSection() {
    return (
        <Container>
            <Spin indicator={<StyledLoading />} />
        </Container>
    );
}
