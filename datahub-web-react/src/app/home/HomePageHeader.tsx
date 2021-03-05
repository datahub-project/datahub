import React from 'react';
import { useHistory } from 'react-router';
import { Typography, Image, Space, AutoComplete, Input, Row } from 'antd';
import styled from 'styled-components';

import { ManageAccount } from '../shared/ManageAccount';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { useEntityRegistry } from '../useEntityRegistry';
import { navigateToSearchUrl } from '../search/utils/navigateToSearchUrl';
import { useGetAutoCompleteResultsLazyQuery } from '../../graphql/search.generated';
import themeConfig from '../../conf/theme/themeConfig';

const Background = styled(Space)`
    width: 100%;
    background-image: linear-gradient(
        ${(props) => props.theme.appVariables.homepage.backgroundColorUpperFade},
        ${(props) => props.theme.appVariables.homepage.backgroundColorLowerFade}
    );
`;

const WelcomeText = styled(Typography.Text)`
    font-size: 16px;
    color: ${(props) => props.theme.appVariables.homepage.backgroundColorLowerFade};
`;

const styles = {
    navBar: { padding: '24px' },
    searchContainer: { width: '100%', marginTop: '40px', marginBottom: '160px' },
    logoImage: { width: 140 },
    searchBox: { width: 540, margin: '40px 0px' },
    subHeaderText: { color: '#FFFFFF', fontSize: 20 },
};

export const HomePageHeader = () => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { data } = useGetAuthenticatedUser();
    const [getAutoCompleteResults, { data: suggestionsData }] = useGetAutoCompleteResultsLazyQuery();

    const onSearch = (query: string) => {
        navigateToSearchUrl({
            query,
            history,
            entityRegistry,
        });
    };

    const onAutoComplete = (query: string) => {
        getAutoCompleteResults({
            variables: {
                input: {
                    type: entityRegistry.getDefaultSearchEntityType(),
                    query,
                },
            },
        });
    };

    return (
        <Background direction="vertical">
            <Row justify="space-between" style={styles.navBar}>
                <WelcomeText>
                    Welcome back, <b>{data?.corpUser?.info?.firstName || data?.corpUser?.username}</b>.
                </WelcomeText>
                <ManageAccount
                    urn={data?.corpUser?.urn || ''}
                    pictureLink={data?.corpUser?.editableInfo?.pictureLink || ''}
                />
            </Row>
            <Space direction="vertical" align="center" style={styles.searchContainer}>
                <Image src={themeConfig.appVariables.logoUrl} preview={false} style={styles.logoImage} />
                <AutoComplete
                    style={styles.searchBox}
                    options={suggestionsData?.autoComplete?.suggestions.map((result: string) => ({
                        value: result,
                    }))}
                    onSelect={(value: string) => onSearch(value)}
                    onSearch={(value: string) => onAutoComplete(value)}
                >
                    <Input.Search
                        placeholder={themeConfig.appVariables.search.searchbarMessage}
                        onSearch={(value: string) => onSearch(value)}
                        data-testid="search-input"
                    />
                </AutoComplete>

                <Typography.Text style={styles.subHeaderText}>
                    {themeConfig.appVariables.homepage.homepageMessage}
                </Typography.Text>
            </Space>
        </Background>
    );
};
