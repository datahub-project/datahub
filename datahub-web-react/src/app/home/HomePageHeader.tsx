import React from 'react';
import { useHistory } from 'react-router';
import { Typography, Image, Space, AutoComplete, Input, Row } from 'antd';
import { ManageAccount } from '../shared/ManageAccount';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { GlobalCfg, SearchCfg } from '../../conf';
import { useEntityRegistry } from '../useEntityRegistry';
import { navigateToSearchUrl } from '../search/utils/navigateToSearchUrl';
import { useGetAutoCompleteResultsLazyQuery } from '../../graphql/search.generated';

const styles = {
    background: {
        width: '100%',
        backgroundImage: 'linear-gradient(#132935, #FFFFFF)',
    },
    navBar: { padding: '24px' },
    welcomeText: { color: '#FFFFFF', fontSize: 16 },
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
        <Space direction="vertical" style={styles.background}>
            <Row justify="space-between" style={styles.navBar}>
                <Typography.Text style={styles.welcomeText}>
                    Welcome back, <b>{data?.corpUser?.info?.firstName || data?.corpUser?.username}</b>.
                </Typography.Text>
                <ManageAccount
                    urn={data?.corpUser?.urn || ''}
                    pictureLink={data?.corpUser?.editableInfo?.pictureLink || ''}
                />
            </Row>
            <Space direction="vertical" align="center" style={styles.searchContainer}>
                <Image src={GlobalCfg.LOGO_IMAGE} preview={false} style={styles.logoImage} />
                <AutoComplete
                    style={styles.searchBox}
                    options={suggestionsData?.autoComplete?.suggestions.map((result: string) => ({
                        value: result,
                    }))}
                    onSelect={(value: string) => onSearch(value)}
                    onSearch={(value: string) => onAutoComplete(value)}
                >
                    <Input.Search
                        placeholder={SearchCfg.SEARCH_BAR_PLACEHOLDER_TEXT}
                        onSearch={(value: string) => onSearch(value)}
                    />
                </AutoComplete>

                <Typography.Text style={styles.subHeaderText}>
                    Find <b>data</b> you can count on.
                </Typography.Text>
            </Space>
        </Space>
    );
};
