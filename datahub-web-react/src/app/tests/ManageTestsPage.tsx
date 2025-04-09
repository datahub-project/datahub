import React, { useEffect, useState } from 'react';
import { PageTitle, Input } from '@src/alchemy-components';
import { message } from 'antd';
import { useLocation } from 'react-router';
import * as QueryString from 'query-string';
import { Tests } from './Tests';
import { DEFAULT_TESTS_PAGE_SIZE } from './constants';
import { useListTestsQuery } from '../../graphql/test.generated';
import { Message } from '../shared/Message';
import { filterTests } from './utils';
import { NewTestButton } from './NewTestButton';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';
import {
    ButtonContainer,
    PageContainer,
    HeaderContainer,
    HeaderContent,
} from '../govern/structuredProperties/styledComponents';

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
            <Input
                label=""
                placeholder="Search..."
                icon={{ icon: 'MagnifyingGlass', source: 'phosphor' }}
                value={searchValue}
                setValue={setSearchValue}
                style={{ maxWidth: '330px' }}
            />
            <Tests tests={filteredTests} />
        </PageContainer>
    );
};
