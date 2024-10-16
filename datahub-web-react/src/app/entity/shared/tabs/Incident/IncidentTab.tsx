import React, { useState } from 'react';
import styled from 'styled-components';
import { Button, Empty, List, Select, Typography } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { useGetEntityIncidentsQuery } from '../../../../../graphql/incident.generated';
import TabToolbar from '../../components/styled/TabToolbar';
import { useEntityData } from '../../EntityContext';
import IncidentListItem from './components/IncidentListItem';
import { INCIDENT_DISPLAY_STATES, PAGE_SIZE, getIncidentsStatusSummary } from './incidentUtils';
import { EntityType, Incident, IncidentState } from '../../../../../types.generated';
import { IncidentSummary } from './components/IncidentSummary';
import { AddIncidentModal } from './components/AddIncidentModal';
import { combineEntityDataWithSiblings } from '../../siblingUtils';
import { IncidentsLoadingSection } from './components/IncidentsLoadingSection';
import { ANTD_GRAY } from '../../constants';

const Header = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[3]};
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
`;

const Summary = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const IncidentList = styled.div`
    flex: 1;
    height: 100%;
    overflow: scroll;
`;

const IncidentStyledList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        flex: 1;
    }
`;

const IncidentStateSelect = styled(Select)`
    width: 100px;
    margin: 0px 40px;
`;

export const IncidentTab = () => {
    const { urn, entityType } = useEntityData();
    const incidentStates = INCIDENT_DISPLAY_STATES;
    const [selectedIncidentState, setSelectedIncidentState] = useState<IncidentState | undefined>(IncidentState.Active);
    const [isRaiseIncidentModalVisible, setIsRaiseIncidentModalVisible] = useState(false);

    // Fetch filtered incidents.
    const { loading, data, refetch } = useGetEntityIncidentsQuery({
        variables: {
            urn,
            start: 0,
            count: PAGE_SIZE,
        },
        fetchPolicy: 'cache-and-network',
    });

    const hasData = (data?.entity as any)?.incidents;
    const combinedData = (entityType === EntityType.Dataset && combineEntityDataWithSiblings(data)) || data;
    const allIncidents =
        (combinedData && (combinedData as any).entity?.incidents?.incidents?.map((incident) => incident as Incident)) ||
        [];
    const filteredIncidents = allIncidents.filter(
        (incident) => !selectedIncidentState || incident.status?.state === selectedIncidentState,
    );
    const incidentList = filteredIncidents?.map((incident) => ({
        urn: incident?.urn,
        created: incident.created,
        customType: incident.customType,
        description: incident.description,
        status: incident.status,
        type: incident?.incidentType,
        title: incident?.title,
    }));

    return (
        <>
            <Header>
                <TabToolbar>
                    <Button icon={<PlusOutlined />} onClick={() => setIsRaiseIncidentModalVisible(true)} type="text">
                        Raise Incident
                    </Button>
                    <AddIncidentModal
                        refetch={refetch}
                        open={isRaiseIncidentModalVisible}
                        onClose={() => setIsRaiseIncidentModalVisible(false)}
                    />
                </TabToolbar>
                <Summary>
                    <IncidentSummary summary={getIncidentsStatusSummary(allIncidents)} />
                    <IncidentStateSelect
                        value={selectedIncidentState}
                        onChange={(newState: any) => setSelectedIncidentState(newState)}
                        autoFocus
                    >
                        {incidentStates.map((incidentType) => {
                            return (
                                <Select.Option key={incidentType.type} value={incidentType.type}>
                                    <Typography.Text>{incidentType.name}</Typography.Text>
                                </Select.Option>
                            );
                        })}
                    </IncidentStateSelect>
                </Summary>
            </Header>

            {(loading && !hasData && <IncidentsLoadingSection />) || null}
            {hasData && (
                <IncidentList>
                    <IncidentStyledList
                        bordered
                        locale={{
                            emptyText: (
                                <Empty
                                    description={`No${
                                        selectedIncidentState ? ` ${selectedIncidentState.toLocaleLowerCase()} ` : ''
                                    } incidents`}
                                    image={Empty.PRESENTED_IMAGE_SIMPLE}
                                />
                            ),
                        }}
                        dataSource={incidentList}
                        renderItem={(item: any) => <IncidentListItem refetch={refetch} incident={item} />}
                    />
                </IncidentList>
            )}
        </>
    );
};
