import { Tooltip } from '@components';
import React, { useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import {
    FAILING_ASSERTION_TYPE_FILTER_FIELD,
    HAS_FAILING_ASSERTIONS_FILTER_FIELD,
    TYPE_TO_DISPLAY_NAME,
} from '@app/observe/dataset/assertion/constants';
import { buildAssertionTypeFilters } from '@app/observe/dataset/assertion/util';
import { MultiDropdownSelect } from '@app/observe/dataset/shared/MultiDropdownSelect';
import { SearchSection } from '@app/observe/dataset/shared/SearchSection';
import {
    DescriptionText,
    Header,
    List,
    Percent,
    Stat,
    Title,
    TitleText,
    Total,
} from '@app/observe/dataset/shared/shared';

import { useAggregateAcrossEntitiesQuery } from '@graphql/search.generated';
import { EntityType, IncidentType } from '@types';

type Props = {
    total: number;
};

/**
 * A component which displays a summary of the datasets that are failing some assertions globally
 */
export const AssertionsSummary = ({ total }: Props) => {
    const userContext = useUserContext();
    const [failingAssertionsTotal, setFailingAssertionsTotal] = useState(0);
    const [selectedAssertionTypes, setSelectedAssertionTypes] = useState<undefined | IncidentType[]>(undefined);
    const viewUrn = userContext.localState?.selectedViewUrn;

    const { data } = useAggregateAcrossEntitiesQuery({
        variables: {
            input: {
                query: '*',
                types: [EntityType.Dataset],
                orFilters: [{ and: [{ field: HAS_FAILING_ASSERTIONS_FILTER_FIELD, value: 'true' }] }],
                facets: [FAILING_ASSERTION_TYPE_FILTER_FIELD],
                searchFlags: {
                    skipCache: true,
                },
                viewUrn,
            },
        },
    });

    const typesFacet =
        data?.aggregateAcrossEntities?.facets?.filter((facet) => facet.field === FAILING_ASSERTION_TYPE_FILTER_FIELD) ||
        [];
    const types = (typesFacet.length && typesFacet[0].aggregations) || [];

    const onTotalChanged = (newTotal: number) => {
        setFailingAssertionsTotal(newTotal);
    };

    const onChangeSelectedAssertionTypes = (newTypes) => {
        if (newTypes && newTypes.length) {
            setSelectedAssertionTypes(newTypes);
        } else {
            setSelectedAssertionTypes(undefined);
        }
    };

    const percentageMatch = (total && ((failingAssertionsTotal / total) * 100).toFixed(0)) || 0;

    return (
        <>
            <Header>
                <Title>
                    <TitleText level={3}>Assertions</TitleText>
                </Title>
                <Stat>
                    <Total>{failingAssertionsTotal}</Total>
                    <Percent>
                        (
                        <Tooltip title="The percentage of all datasets with failing assertions.">
                            {percentageMatch}%
                        </Tooltip>
                        )
                    </Percent>
                </Stat>
                <DescriptionText>
                    datasets <b>failing assertions</b> of type{'  '}
                    <MultiDropdownSelect
                        value={selectedAssertionTypes}
                        options={types.map((type) => ({
                            name: `${
                                TYPE_TO_DISPLAY_NAME.has(type.value) ? TYPE_TO_DISPLAY_NAME.get(type.value) : type.value
                            } (${type.count})`,
                            value: type.value,
                        }))}
                        onChange={onChangeSelectedAssertionTypes}
                    />
                </DescriptionText>
            </Header>
            <List>
                <SearchSection
                    onTotalChanged={onTotalChanged}
                    fixedFilters={buildAssertionTypeFilters(selectedAssertionTypes)}
                />
            </List>
        </>
    );
};
