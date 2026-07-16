import { Skeleton } from 'antd';
import * as React from 'react';
import styled, { useTheme } from 'styled-components';

const Container = styled.div`
    width: 100%;
    display: flex;
    flex-direction: column;
    padding: 24px 0px 20px 40px;
    overflow: auto;
    flex: 1;
`;

export default function SearchResultsLoadingSection() {
    const theme = useTheme();
    const cardStyle = {
        backgroundColor: theme.colors.bgSurface,
        height: 120,
        minWidth: '98%',
        borderRadius: 8,
        marginBottom: 20,
    };
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
