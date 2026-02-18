import { LoadingOutlined } from '@ant-design/icons';
import { Spin } from 'antd';
import * as React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    height: auto;
    width: 100%;
    display: flex;
    justify-content: center;
    align-items: center;
    background-color: ${(props) => props.theme.colors.bg};
`;

const StyledLoading = styled(LoadingOutlined)`
    font-size: 32px;
    color: ${(props) => props.theme.colors.textTertiary};
    padding-bottom: 18px;
]`;

export default function LineageLoadingSection() {
    return (
        <Container>
            <Spin indicator={<StyledLoading />} />
        </Container>
    );
}
