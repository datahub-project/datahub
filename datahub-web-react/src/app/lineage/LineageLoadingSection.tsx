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
