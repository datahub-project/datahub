import React from 'react';
import { useTranslation } from 'react-i18next';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { HeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';
import { HorizontalList } from '@app/entityV2/shared/summary/ListComponents';
import { CONTAINER_FILTER_NAME, TYPE_NAMES_FILTER_NAME } from '@app/searchV2/utils/constants';
import SummaryEntityCard from '@app/sharedV2/cards/SummaryEntityCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

const QLIK_SENSE_PLATFORM = 'qlik-sense';
// A Qlik app's own dashboard sits in the app beside its sheets.
const QLIK_APP_DASHBOARD_SUBTYPE = 'App';

/** The dashboards in a workbook container, such as a Qlik app's sheets. */
export default function WorkbookDashboardsSection() {
    const { urn, entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const { t } = useTranslation('entity.types');
    const isQlik = entityData?.platform?.name === QLIK_SENSE_PLATFORM;

    const { data } = useGetSearchResultsForMultipleQuery({
        skip: !urn,
        variables: {
            input: {
                types: [EntityType.Dashboard],
                query: '',
                orFilters: [
                    {
                        and: [
                            { field: CONTAINER_FILTER_NAME, values: [urn] },
                            ...(isQlik
                                ? [
                                      {
                                          field: TYPE_NAMES_FILTER_NAME,
                                          values: [QLIK_APP_DASHBOARD_SUBTYPE],
                                          negated: true,
                                      },
                                  ]
                                : []),
                        ],
                    },
                ],
                count: 1000,
            },
        },
        fetchPolicy: 'cache-first',
    });

    const dashboards = data?.searchAcrossEntities?.searchResults.map((r) => r.entity);
    if (!dashboards?.length) {
        return null;
    }
    return (
        <>
            <HeaderTitle>
                {isQlik ? t('container.workbookSheets') : entityRegistry.getCollectionName(EntityType.Dashboard)} (
                {dashboards.length})
            </HeaderTitle>
            <HorizontalList>
                {dashboards.map((dashboard) => (
                    <SummaryEntityCard key={dashboard.urn} entity={dashboard} />
                ))}
            </HorizontalList>
        </>
    );
}
