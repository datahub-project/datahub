import { Empty } from 'antd';
import React, { useEffect, useState } from 'react';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { combineEntityDataWithSiblings } from '@app/entity/shared/siblingUtils';
import { getQueryParams } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';
import { IncidentDetailDrawer } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentDetailDrawer';
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

export const IncidentList = () => {
    const { urn } = useEntityData();
    const refetchEntity = useRefetch();
    const [showIncidentBuilder, setShowIncidentBuilder] = useState(false);
    const [entity, setEntity] = useState<EntityStagedForIncident>();
    const [visibleIncidents, setVisibleIncidents] = useState<IncidentTable>({
        incidents: [],
        groupBy: { type: [], priority: [], stage: [], state: [] },
    });
    const [allIncidentData, setAllIncidentData] = useState<Incident[]>([]);

    const isSeparateSiblingsMode = useIsSeparateSiblingsMode();
    const incidentUrnParam = getQueryParams('incident_urn', window.location);
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

    useEffect(() => {
        const combinedData = isSeparateSiblingsMode ? data : combineEntityDataWithSiblings(data);
        const allIncidents =
            (combinedData &&
                (combinedData as any).entity?.incidents?.incidents?.map((incident) => incident as Incident)) ||
            [];
        setAllIncidentData(allIncidents);
        getFilteredIncidents(allIncidents);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [data]);

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
        refetchEntity();
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
        return <Empty description="No incidents yet" image={Empty.PRESENTED_IMAGE_SIMPLE} />;
    };

    return (
        <>
            <IncidentTitleContainer
                privileges={privileges}
                setShowIncidentBuilder={setShowIncidentBuilder}
                setEntity={setEntity}
            />
            {allIncidentData?.length > 0 && !loading && (
                <IncidentFilterContainer
                    filteredIncidents={visibleIncidents}
                    originalFilterOptions={visibleIncidents?.originalFilterOptions}
                    handleFilterChange={handleFilterChange}
                    selectedFilters={selectedFilters}
                />
            )}
            {renderListTable()}
            {showIncidentBuilder && (
                <IncidentDetailDrawer
                    urn={urn}
                    mode={IncidentAction.CREATE}
                    onSubmit={() => {
                        setShowIncidentBuilder(false);
                        setTimeout(() => {
                            refetch();
                        }, 3000);
                    }}
                    onCancel={() => setShowIncidentBuilder(false)}
                    entity={entity}
                />
            )}
        </>
    );
};
