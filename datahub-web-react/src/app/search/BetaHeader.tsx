import * as React from 'react';
import styled from 'styled-components';
import { Layout, Typography } from 'antd';

const HeaderTitle = styled(Typography.Title)`
    && {
        color: ${(props) => props.theme.styles['layout-header-color']};
        padding-left: 12px;
        margin: 0;
    }
`;

const { Header } = Layout;

const styles = {
    header: {
        zIndex: 1,
        width: '100%',
        height: '80px',
        marginTop: '80px',
        lineHeight: '20px',
        padding: '0px 40px',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        backgroundColor: '#555555',
    },
};

/**
 * A header with just a text announcement.
 */
export const BetaHeader = () => {
    return (
        <Header style={styles.header as any}>
            <HeaderTitle level={4}>
                This is a Beta release of the data catalog. More information &nbsp;
                <a href="https://klub.klarna.net/team/data-exploration-security/?page_id=311">here</a>. General
                feedback&nbsp;
                <a href="https://docs.google.com/forms/d/1U5mTbLDanpwbppx9afwPk5Adi1IctsGWYRRfOh0r7Dg">here</a>. Dataset
                quality feedback&nbsp;
                <a href="https://docs.google.com/forms/d/1UtWZEo0Tpx5BsPxMAM1Rl82_WVeJdwwbbHkHnGxhNLM">here</a>.
            </HeaderTitle>
        </Header>
    );
};
