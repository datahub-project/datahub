import { message } from 'antd';
import * as QueryString from 'query-string';
import React, { useEffect, useState } from 'react';
import { useLocation } from 'react-router';

import {
    ButtonContainer,
    HeaderContainer,
    HeaderContent,
    PageContainer,
} from '@app/govern/structuredProperties/styledComponents';
import { Message } from '@app/shared/Message';
import { NewTestButton } from '@app/tests/NewTestButton';
import { Tests } from '@app/tests/Tests';
import { DEFAULT_TESTS_PAGE_SIZE } from '@app/tests/constants';
import { filterTests } from '@app/tests/utils';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageTitle, SearchBar } from '@src/alchemy-components';

import { useListTestsQuery } from '@graphql/test.generated';

export const ManageTestsPage = () => {
    const location = useLocation();
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const paramsQuery = (params?.query as string) || undefined;
    const [filterText, setFilterText] = useState<undefined | string>(undefined);
    useEffect(() => setFilterText(paramsQuery), [paramsQuery]);

    const [searchValue, setSearchValue] = useState('');

    useEffect(() => {
        setFilterText(searchValue === '' ? undefined : searchValue);
    }, [searchValue]);

    useEffect(() => {
        setSearchValue(filterText || '');
    }, [filterText]);

    /**
     * We always fetch 1,000 tests initially to make the following experience snappier.
     */
    const { loading, error, data } = useListTestsQuery({
        variables: {
            input: {
                start: 0,
                count: DEFAULT_TESTS_PAGE_SIZE,
            },
        },
        fetchPolicy: 'cache-first',
    });
    const tests = data?.listTests?.tests || [];
    const filteredTests = filterTests(filterText, tests as any);

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            {!data && loading && <Message type="loading" content="Loading tests..." />}
            {error && message.error({ content: `Failed to load Tests! An unexpected error occurred.`, duration: 3 })}
            <HeaderContainer>
                <HeaderContent>
                    <PageTitle
                        title="Metadata Tests"
                        subTitle="Discover & monitor data assets matching a set of logical conditions."
                    />
                </HeaderContent>
                <ButtonContainer>
                    <NewTestButton />
                </ButtonContainer>
            </HeaderContainer>
            <SearchBar
                placeholder="Search..."
                value={searchValue}
                onChange={setSearchValue}
                style={{ width: '330px' }}
            />
            <Tests tests={filteredTests} />
        </PageContainer>
    );
};
