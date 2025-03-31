import React, { useEffect, useState } from 'react';
import { Empty } from 'antd';

import { useGetEntityIncidentsQuery } from '../../../../../graphql/incident.generated';
import { useEntityData } from '../../../../entity/shared/EntityContext';
import { PAGE_SIZE } from './incidentUtils';
import { EntityPrivileges, Incident } from '../../../../../types.generated';
import { combineEntityDataWithSiblings } from '../../../../entity/shared/siblingUtils';
import { useIsSeparateSiblingsMode } from '../../useIsSeparateSiblingsMode';
import { IncidentTitleContainer } from './IncidentTitleContainer';
import { EntityStagedForIncident, IncidentListFilter, IncidentTable } from './types';
import { INCIDENT_DEFAULT_FILTERS, IncidentAction } from './constant';
import { IncidentFilterContainer } from './IncidentFilterContainer';
import { IncidentListTable } from './IncidentListTable';
import { getFilteredTransformedIncidentData } from './utils';
import { IncidentDetailDrawer } from './AcrylComponents/IncidentDetailDrawer';
import { IncidentListLoading } from './IncidentListLoading';
import { getQueryParams } from '../Dataset/Validations/assertionUtils';

export const IncidentList = () => {
    const { urn } = useEntityData();
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
    const { loading, data, refetch } = useGetEntityIncidentsQuery({
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
                        setTimeout(() => {
                            refetch();
                        }, 2000);
                        setShowIncidentBuilder(false);
                    }}
                    onCancel={() => setShowIncidentBuilder(false)}
                    entity={entity}
                />
            )}
        </>
    );
};
