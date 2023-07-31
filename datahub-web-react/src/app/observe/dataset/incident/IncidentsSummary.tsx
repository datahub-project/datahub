import React, { useState } from 'react';
import styled from 'styled-components';
import { FlagOutlined } from '@ant-design/icons';
import { Tooltip } from 'antd';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { SearchSection } from '../shared/SearchSection';
import { EntityType, IncidentType } from '../../../../types.generated';
import { MultiDropdownSelect } from '../shared/MultiDropdownSelect';
import { useAggregateAcrossEntitiesQuery } from '../../../../graphql/search.generated';
import {
    ACTIVE_INCIDENT_TYPES_FILTER_FIELD,
    HAS_ACTIVE_INCIDENTS_FILTER_FIELD,
    TYPE_TO_DISPLAY_NAME,
} from './constants';
import { buildIncidentTypeFilters } from './util';
import { Stat, List, Header, Title, TitleText, DescriptionText, Percent, Total } from '../shared/shared';

const StyledFlagOutlined = styled(FlagOutlined)`
    margin-right: 8px;
    font-size: 20px;
    color: ${ANTD_GRAY[7]};
`;

type Props = {
    total: number;
};

/**
 * A component which displays a summary of the datasets that have active incidents globally
 */
export const IncidentsSummary = ({ total }: Props) => {
    const [failingIncidentsTotal, setFailingIncidentsTotal] = useState(0);
    const [selectedIncidentTypes, setSelectedIncidentTypes] = useState<undefined | IncidentType[]>(undefined);

    const { data } = useAggregateAcrossEntitiesQuery({
        variables: {
            input: {
                query: '*',
                types: [EntityType.Dataset],
                orFilters: [{ and: [{ field: HAS_ACTIVE_INCIDENTS_FILTER_FIELD, value: 'true' }] }],
                facets: [ACTIVE_INCIDENT_TYPES_FILTER_FIELD],
                searchFlags: {
                    skipCache: true,
                },
            },
        },
    });

    const typesFacet =
        data?.aggregateAcrossEntities?.facets?.filter((facet) => facet.field === ACTIVE_INCIDENT_TYPES_FILTER_FIELD) ||
        [];
    const types = (typesFacet.length && typesFacet[0].aggregations) || [];

    const onTotalChanged = (newTotal: number) => {
        setFailingIncidentsTotal(newTotal);
    };

    const onChangeSelectedIncidentTypes = (newTypes) => {
        if (newTypes && newTypes.length) {
            setSelectedIncidentTypes(newTypes);
        } else {
            setSelectedIncidentTypes(undefined);
        }
    };

    const percentageMatch = (total && ((failingIncidentsTotal / total) * 100).toFixed(0)) || 0;

    return (
        <>
            <Header>
                <Title>
                    <StyledFlagOutlined />
                    <TitleText level={3}>Incidents</TitleText>
                </Title>
                <Stat>
                    <Total>{failingIncidentsTotal} </Total>
                    <Percent>
                        (
                        <Tooltip title="The percentage of all datasets with active incidents.">
                            {percentageMatch}%
                        </Tooltip>
                        )
                    </Percent>
                </Stat>
                <DescriptionText>
                    datasets with <b>active incidents</b> of type{' '}
                    <MultiDropdownSelect
                        value={selectedIncidentTypes}
                        options={types.map((type) => ({
                            name: `${
                                TYPE_TO_DISPLAY_NAME.has(type.value) ? TYPE_TO_DISPLAY_NAME.get(type.value) : type.value
                            } (${type.count})`,
                            value: type.value,
                        }))}
                        onChange={onChangeSelectedIncidentTypes}
                    />
                </DescriptionText>
            </Header>
            <List>
                <SearchSection
                    onTotalChanged={onTotalChanged}
                    fixedFilters={buildIncidentTypeFilters(selectedIncidentTypes)}
                />
            </List>
        </>
    );
};
