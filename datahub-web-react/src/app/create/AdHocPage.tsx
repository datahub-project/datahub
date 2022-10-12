import React from 'react';
import { Alert, Layout, Typography } from 'antd';
import styled from 'styled-components';
import { Content } from 'antd/lib/layout/layout';
import { SearchablePage } from '../search/SearchablePage';
import { CsvForm } from './Components/CsvForm';
import { env } from '../../env';

const Title = styled(Typography.Text)`
    && {
        font-size: 32px;
        color: ${(props) => props.theme.styles['homepage-background-upper-fade']};
    }
`;

export const AdHocPage = () => {
    return (
        <>
            <SearchablePage>
                <Layout>
                    <Content style={{ padding: '0 50px' }}>
                        <Layout className="site-layout-background" style={{ padding: '24px 0' }}>
                            <Content style={{ padding: '0 24px', minHeight: 280 }}>
                                <Alert
                                    message={
                                        <span>
                                            Not sure how to onboard your dataset? Refer to our guide{' '}
                                            <a href={env.GUIDE} target="_blank" rel="noopener noreferrer">
                                                here
                                            </a>
                                        </span>
                                    }
                                    type="info"
                                    closeText="Close Now"
                                />
                                <br />
                                <Title>
                                    <b>Create </b>
                                    your own dataset
                                </Title>
                                <br />
                                <CsvForm />
                            </Content>
                        </Layout>
                    </Content>
                </Layout>
            </SearchablePage>
        </>
    );
};
