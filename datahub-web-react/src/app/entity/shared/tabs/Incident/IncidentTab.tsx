import React, { useState } from 'react';
import styled from 'styled-components';
import { Button, Empty, List, Pagination, Select, Typography } from 'antd';
import { PlusOutlined } from '@ant-design/icons';

import { useGetIncidentsQuery } from '../../../../../graphql/dataset.generated';
import TabToolbar from '../../components/styled/TabToolbar';
import { useEntityData } from '../../EntityContext';
import { ReactComponent as LoadingSvg } from '../../../../../images/datahub-logo-color-loading_pendulum.svg';
import IncidentListItem from './components/IncidentListItem';
import { INCIDENT_DISPLAY_STATES } from './incidentUtils';
import { Incident, IncidentState } from '../../../../../types.generated';
import { IncidentSummary } from './components/IncidentSummary';
import { AddIncidentModal } from './components/AddIncidentModal';
import { combineEntityDataWithSiblings } from '../../siblingUtils';

const LoadingContainer = styled.div`
    padding-top: 40px;
    padding-bottom: 40px;
    width: 100%;
    text-align: center;
`;

const LoadingText = styled.div`
    margin-top: 18px;
    font-size: 12px;
`;

const IncidentStyledList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
    }
`;

const PaginationControlContainer = styled.div`
    padding-top: 16px;
    padding-bottom: 16px;
    text-align: center;
`;

const IncidentStateSelect = styled(Select)`
    width: 100px;
`;

const PAGE_SIZE = 10;

/**
 * Returns a status summary for the incidents
 */
const getIncidentsStatusSummary = (incidents: Array<Incident>) => {
    const summary = {
        resolvedIncident: 0,
        activeIncident: 0,
        totalIncident: 0,
    };
    incidents.forEach((assertion) => {
        if (incidents.length) {
            const resultType = assertion.status.state;
            if (IncidentState.Active === resultType) {
                summary.activeIncident++;
            }
            if (IncidentState.Resolved === resultType) {
                summary.resolvedIncident++;
            }
            summary.totalIncident++;
        }
    });
    return summary;
};

/**
 * Function to map selected incident to query incident
 */
const getIncidentState = (selectedIncidentState: string | undefined) => {
    let incidentState: IncidentState | undefined;
    if (selectedIncidentState === 'All') {
        incidentState = undefined;
    } else if (selectedIncidentState === 'ACTIVE') {
        incidentState = IncidentState.Active;
    } else if (selectedIncidentState === 'RESOLVED') {
        incidentState = IncidentState.Resolved;
    }
    return incidentState;
};

export const IncidentTab = () => {
    const { urn } = useEntityData();
    const [page, setPage] = useState(1);
    const incidentStates = INCIDENT_DISPLAY_STATES;
    const [selectedIncidentState, setSelectedIncidentState] = useState<string>('All');
    const [isRaiseIncidentModalVisible, setIsRaiseIncidentModalVisible] = useState(false);

    // Fetching the incidents data using Incidents Query
    const { loading, data, refetch } = useGetIncidentsQuery({
        variables: {
            urn,
            start: (page - 1) * PAGE_SIZE,
            count: PAGE_SIZE,
            state: getIncidentState(selectedIncidentState),
        },
    });

    const combinedData = combineEntityDataWithSiblings(data);

    const incidents =
        (combinedData && combinedData.dataset?.incidents?.incidents?.map((incident) => incident as Incident)) || [];

    const incidentList = incidents?.map((incident) => ({
        urn: incident?.urn,
        created: incident.created,
        customType: incident.customType,
        description: incident.description,
        status: incident.status,
        type: incident?.type,
        title: incident?.title,
    }));

    const onSelectIncidentState = (newState) => {
        setSelectedIncidentState(newState);
    };

    return (
        <>
            <TabToolbar>
                <Button icon={<PlusOutlined />} onClick={() => setIsRaiseIncidentModalVisible(true)} type="text">
                    Raise Incident
                </Button>
                <AddIncidentModal
                    refetch={refetch}
                    visible={isRaiseIncidentModalVisible}
                    onClose={() => setIsRaiseIncidentModalVisible(false)}
                />
                <IncidentStateSelect value={selectedIncidentState} onChange={onSelectIncidentState} autoFocus>
                    {incidentStates.map((incidentType) => {
                        return (
                            <Select.Option key={incidentType.type} value={incidentType.type}>
                                <Typography.Text>{incidentType.name}</Typography.Text>
                            </Select.Option>
                        );
                    })}
                </IncidentStateSelect>
            </TabToolbar>
            <IncidentSummary summary={getIncidentsStatusSummary(incidents)} />
            {loading && (
                <LoadingContainer>
                    <LoadingSvg height={80} width={80} />
                    <LoadingText>Fetching Incidents...</LoadingText>
                </LoadingContainer>
            )}
            {!loading && (
                <>
                    <IncidentStyledList
                        bordered
                        locale={{
                            emptyText: <Empty description="No Incidents!" image={Empty.PRESENTED_IMAGE_SIMPLE} />,
                        }}
                        dataSource={incidentList}
                        renderItem={(item: any) => <IncidentListItem refetch={refetch} incident={item} />}
                    />
                    <PaginationControlContainer>
                        <Pagination
                            current={page}
                            pageSize={PAGE_SIZE}
                            total={combinedData?.dataset?.incidents?.total || 0}
                            showLessItems
                            onChange={(newPage) => setPage(newPage)}
                            showSizeChanger={false}
                        />
                    </PaginationControlContainer>
                </>
            )}
        </>
    );
};
