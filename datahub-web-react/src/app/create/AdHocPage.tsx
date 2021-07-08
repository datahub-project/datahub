import React, { useState } from 'react';
import { Card, Layout, Typography } from 'antd';
import styled from 'styled-components';
import { Content } from 'antd/lib/layout/layout';
import { SearchablePage } from '../search/SearchablePage';
import { JsonForm } from './Components/JsonForm';
import { CsvForm } from './Components/CsvForm';

const Title = styled(Typography.Text)`
    && {
        font-size: 32px;
        color: ${(props) => props.theme.styles['homepage-background-upper-fade']};
    }
`;

export const AdHocPage = () => {
    const [state, setState] = useState({
        key: 'tab1',
        titleKey: 'csvForm',
    });
    const onTabChange = (key, type) => {
        console.log(key, type);
        setState((prevState) => ({ key: prevState.key, titleKey: key }));
    };
    const contentList = {
        jsonForm: <JsonForm />,
        csvForm: <CsvForm />,
    };
    const tabList = [
        {
            key: 'csvForm',
            tab: 'Csv',
        },
        {
            key: 'jsonForm',
            tab: 'Json',
        },
    ];
    return (
        <>
            <SearchablePage>
                <Layout>
                    <Content style={{ padding: '0 50px' }}>
                        <Layout className="site-layout-background" style={{ padding: '24px 0' }}>
                            <Content style={{ padding: '0 24px', minHeight: 280 }}>
                                <Title>
                                    <b>Create </b>
                                    your own dataset
                                </Title>
                                <br />
                                <br />
                                <Card
                                    style={{ width: '100%' }}
                                    tabList={tabList}
                                    activeTabKey={state.titleKey}
                                    onTabChange={(key) => {
                                        onTabChange(key, 'noTitleKey');
                                    }}
                                >
                                    {contentList[state.titleKey]}
                                </Card>
                            </Content>
                        </Layout>
                    </Content>
                </Layout>
            </SearchablePage>
        </>
    );
};
