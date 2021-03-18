import React from 'react';
import { useHistory } from 'react-router';
import { Typography, Image, Space, AutoComplete, Input, Row, Button, Carousel } from 'antd';
import styled, { useTheme } from 'styled-components';

import { ManageAccount } from '../shared/ManageAccount';
import { useGetAuthenticatedUser } from '../useGetAuthenticatedUser';
import { useEntityRegistry } from '../useEntityRegistry';
import { navigateToSearchUrl } from '../search/utils/navigateToSearchUrl';
import { GetSearchResultsQuery, useGetAutoCompleteResultsLazyQuery } from '../../graphql/search.generated';
import { useGetAllEntitySearchResults } from '../../utils/customGraphQL/useGetAllEntitySearchResults';
import { EntityType } from '../../types.generated';

const Background = styled(Space)`
    width: 100%;
    background-image: linear-gradient(
        ${(props) => props.theme.styles['homepage-background-upper-fade']},
        75%,
        ${(props) => props.theme.styles['homepage-background-lower-fade']}
    );
`;

const WelcomeText = styled(Typography.Text)`
    font-size: 16px;
    color: ${(props) => props.theme.styles['homepage-background-lower-fade']};
`;

const styles = {
    navBar: { padding: '24px' },
    searchContainer: { width: '100%', marginTop: '40px' },
    logoImage: { width: 140 },
    searchBox: { width: 540, margin: '40px 0px' },
    subHeaderText: { color: '#FFFFFF', fontSize: 20 },
    subHeaderLabel: { marginTop: '-16px', color: '#FFFFFF', fontSize: 12 },
};

const CarouselElement = styled.div`
    height: 100px;
    color: #fff;
    line-height: 100px;
    text-align: center;
`;

const HeaderContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
`;

function getSuggestionFieldsFromResult(result: GetSearchResultsQuery): string[] {
    return (
        (result.search?.entities
            ?.map((entity) => {
                switch (entity.__typename) {
                    case 'Dataset':
                        return entity.name;
                    case 'CorpUser':
                        return entity.username;
                    case 'Chart':
                        return entity.info?.name;
                    case 'Dashboard':
                        return entity.info?.name;
                    default:
                        return undefined;
                }
            })
            .filter(Boolean) as string[]) || []
    );
}

export const HomePageHeader = () => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const { data } = useGetAuthenticatedUser();
    const [getAutoCompleteResults, { data: suggestionsData }] = useGetAutoCompleteResultsLazyQuery();
    const themeConfig = useTheme();

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

    // fetch some results from each entity to display search suggestions
    const allSearchResultsByType = useGetAllEntitySearchResults({
        query: '*',
        start: 0,
        count: 3,
        filters: [],
    });

    const suggestionsLoading = Object.keys(allSearchResultsByType).some((type) => {
        return allSearchResultsByType[type].loading;
    });

    let suggestionsToShow: string[] = [];
    if (!suggestionsLoading) {
        [EntityType.Dashboard, EntityType.Chart, EntityType.Dataset].forEach((type) => {
            const suggestionsToShowForEntity = getSuggestionFieldsFromResult(allSearchResultsByType[type].data);
            const suggestionToAddToFront = suggestionsToShowForEntity?.pop();
            suggestionsToShow = [...suggestionsToShow, ...suggestionsToShowForEntity];
            if (suggestionToAddToFront) {
                suggestionsToShow.splice(0, 0, suggestionToAddToFront);
            }
        });
    }

    return (
        <Background direction="vertical">
            <Row justify="space-between" style={styles.navBar}>
                <WelcomeText>
                    {data && (
                        <>
                            Welcome back, <b>{data?.corpUser?.info?.firstName || data?.corpUser?.username}</b>.
                        </>
                    )}
                </WelcomeText>
                <ManageAccount
                    urn={data?.corpUser?.urn || ''}
                    pictureLink={data?.corpUser?.editableInfo?.pictureLink || ''}
                />
            </Row>
            <HeaderContainer>
                <Image src={themeConfig.assets.logoUrl} preview={false} style={styles.logoImage} />
                <AutoComplete
                    style={styles.searchBox}
                    options={suggestionsData?.autoComplete?.suggestions.map((result: string) => ({
                        value: result,
                    }))}
                    onSelect={(value: string) => onSearch(value)}
                    onSearch={(value: string) => onAutoComplete(value)}
                >
                    <Input.Search
                        placeholder={themeConfig.content.search.searchbarMessage}
                        onSearch={(value: string) => onSearch(value)}
                        data-testid="search-input"
                    />
                </AutoComplete>
                {suggestionsToShow.length === 0 && (
                    <Typography.Text style={styles.subHeaderText}>
                        {themeConfig.content.homepage.homepageMessage}
                    </Typography.Text>
                )}
                <Typography.Text style={styles.subHeaderLabel}>Try searching for...</Typography.Text>
            </HeaderContainer>
            <div>
                <Carousel autoplay>
                    {suggestionsToShow.length > 0 &&
                        suggestionsToShow.map((suggestion) => (
                            <CarouselElement>
                                <Button type="text" style={styles.subHeaderText}>
                                    {suggestion}
                                </Button>
                            </CarouselElement>
                        ))}
                </Carousel>
            </div>
        </Background>
    );
};
