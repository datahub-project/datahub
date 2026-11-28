import { EmptyState } from '@components';
import { Warning } from '@phosphor-icons/react/dist/csr/Warning';
import React, { useEffect, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation } from 'react-router';
import styled from 'styled-components';

import { useEntityData, useRefetch } from '@app/entityV2/shared/EntityContext';
import { combineEntityDataWithSiblings } from '@app/entityV2/shared/siblingUtils';
import { getQueryParams } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import { IncidentDetailDrawer } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentDetailDrawer';
import { CreateIncidentButton } from '@app/entityV2/shared/tabs/Incident/CreateIncidentButton';
import { IncidentFilterContainer } from '@app/entityV2/shared/tabs/Incident/IncidentFilterContainer';
import { IncidentListLoading } from '@app/entityV2/shared/tabs/Incident/IncidentListLoading';
import { IncidentListTable } from '@app/entityV2/shared/tabs/Incident/IncidentListTable';
import { IncidentTitleContainer } from '@app/entityV2/shared/tabs/Incident/IncidentTitleContainer';
import { INCIDENT_DEFAULT_FILTERS, IncidentAction } from '@app/entityV2/shared/tabs/Incident/constant';
import { PAGE_SIZE } from '@app/entityV2/shared/tabs/Incident/incidentUtils';
import { EntityStagedForIncident, IncidentListFilter, IncidentTable } from '@app/entityV2/shared/tabs/Incident/types';
import { getFilteredTransformedIncidentData } from '@app/entityV2/shared/tabs/Incident/utils';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';

import { useGetEntityIncidentsQuery } from '@graphql/incident.generated';
import { EntityPrivileges, Incident } from '@types';

const ListEmptyState = styled(EmptyState)`
    padding: 48px 0;
`;

const TabEmptyState = styled(EmptyState)`
    padding: 80px 20px;
`;

export const IncidentList = () => {
    const { t } = useTranslation('entity.profile.incident');
    const { urn, entityType } = useEntityData();
    const location = useLocation();
    const refetchEntity = useRefetch();
    const [showIncidentBuilder, setShowIncidentBuilder] = useState(false);
    const [entity, setEntity] = useState<EntityStagedForIncident>({
        urn,
        entityType,
    });
    const [visibleIncidents, setVisibleIncidents] = useState<IncidentTable>({
        incidents: [],
        groupBy: { category: [], priority: [], stage: [], state: [] },
    });
    const [allIncidentData, setAllIncidentData] = useState<Incident[]>([]);

    const isSeparateSiblingsMode = useIsSeparateSiblingsMode();
    const incidentUrnParam = getQueryParams('incident_urn', location);
    const incidentDefaultFilters = INCIDENT_DEFAULT_FILTERS;
    if (incidentUrnParam) {
        incidentDefaultFilters.filterCriteria.state = [];
    }

    const [selectedFilters, setSelectedFilters] = useState<IncidentListFilter>(incidentDefaultFilters);
    // Fetch filtered incidents.
    const {
        loading,
        data,
        refetch: refetchIncidents,
    } = useGetEntityIncidentsQuery({
        variables: {
            urn,
            start: 0,
            count: PAGE_SIZE,
        },
        fetchPolicy: 'cache-first',
    });

    // get filtered Incident as per the filter object
    const getFilteredIncidents = (incidents: Incident[]) => {
        const filteredIncidentData: IncidentTable = getFilteredTransformedIncidentData(incidents, selectedFilters);
        setVisibleIncidents(filteredIncidentData);
    };

    const incidentsFromQuery = useMemo(() => {
        const combinedData = isSeparateSiblingsMode ? data : combineEntityDataWithSiblings(data);
        return ((combinedData as any)?.entity?.incidents?.incidents?.map((incident) => incident as Incident) ||
            []) as Incident[];
    }, [data, isSeparateSiblingsMode]);

    useEffect(() => {
        const allIncidents = incidentsFromQuery;
        setAllIncidentData(allIncidents);
        getFilteredIncidents(allIncidents);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [incidentsFromQuery]);

    useEffect(() => {
        // after filter change need to get filtered incidents
        if (allIncidentData?.length > 0) {
            getFilteredIncidents(allIncidentData);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedFilters]);

    const handleFilterChange = (filter) => {
        setSelectedFilters(filter);
    };

    const refetch = () => {
        setTimeout(() => {
            refetchEntity();
        }, 3000);
        refetchIncidents();
    };

    const privileges = (data?.entity as any)?.privileges as EntityPrivileges;

    const renderListTable = () => {
        if (loading) {
            return <IncidentListLoading />;
        }
        if ((visibleIncidents?.incidents || []).length > 0) {
            return (
                <IncidentListTable
                    incidentData={visibleIncidents}
                    filter={selectedFilters}
                    refetch={() => {
                        refetch();
                    }}
                    privileges={privileges}
                />
            );
        }
        return <ListEmptyState icon={Warning} title={t('list.empty')} size="sm" />;
    };

    // With no incidents at all, the empty state stands in for the header and carries the Create action.
    // Read from the query result rather than state, which only updates after an effect and would flash this.
    const hasNoIncidents = !loading && incidentsFromQuery.length === 0;

    return (
        <>
            {hasNoIncidents && (
                <TabEmptyState icon={Warning} title={t('list.empty')} description={t('list.emptyDescription')}>
                    <CreateIncidentButton
                        privileges={privileges}
                        setShowIncidentBuilder={setShowIncidentBuilder}
                        setEntity={setEntity}
                    />
                </TabEmptyState>
            )}
            {!hasNoIncidents && (
                <IncidentTitleContainer
                    privileges={privileges}
                    setShowIncidentBuilder={setShowIncidentBuilder}
                    setEntity={setEntity}
                />
            )}
            {allIncidentData?.length > 0 && !loading && (
                <IncidentFilterContainer
                    filteredIncidents={visibleIncidents}
                    originalFilterOptions={visibleIncidents?.originalFilterOptions}
                    handleFilterChange={handleFilterChange}
                    selectedFilters={selectedFilters}
                />
            )}
            {!hasNoIncidents && renderListTable()}
            {showIncidentBuilder && (
                <IncidentDetailDrawer
                    entity={entity}
                    mode={IncidentAction.CREATE}
                    onSubmit={() => {
                        setShowIncidentBuilder(false);
                        setTimeout(() => {
                            refetch();
                        }, 3000);
                    }}
                    onCancel={() => setShowIncidentBuilder(false)}
                />
            )}
        </>
    );
};
