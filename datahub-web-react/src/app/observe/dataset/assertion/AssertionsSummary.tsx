import React, { useState } from 'react';
import styled from 'styled-components';
import { CheckCircleOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { SearchSection } from '../shared/SearchSection';
import { MultiDropdownSelect } from '../shared/MultiDropdownSelect';
import { useAggregateAcrossEntitiesQuery } from '../../../../graphql/search.generated';
import {
    HAS_FAILING_ASSERTIONS_FILTER_FIELD,
    FAILING_ASSERTION_TYPE_FILTER_FIELD,
    TYPE_TO_DISPLAY_NAME,
} from './constants';
import { buildAssertionTypeFilters } from './util';
import { EntityType, IncidentType } from '../../../../types.generated';
import { Stat, List, Header, Title, TitleText, DescriptionText, Percent, Total } from '../shared/shared';

const StyledCheckCircleOutlined = styled(CheckCircleOutlined)`
    margin-right: 8px;
    font-size: 20px;
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    total: number;
};

/**
 * A component which displays a summary of the datasets that are failing some assertions globally
 */
export const AssertionsSummary = ({ total }: Props) => {
    const [failingAssertionsTotal, setFailingAssertionsTotal] = useState(0);
    const [selectedAssertionTypes, setSelectedAssertionTypes] = useState<undefined | IncidentType[]>(undefined);

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
                    <StyledCheckCircleOutlined />
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
