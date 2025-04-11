import { orange } from '@ant-design/colors';
import { DownOutlined, WarningFilled } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';
import { Typography } from 'antd';
import { Dataset } from '../../../../../types.generated';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { ANTD_GRAY } from '../../constants';
import { getDisplayedEntityType } from '../../containers/profile/header/utils';
import { useEntityData } from '../../../../entity/shared/EntityContext';
import ActiveIncidents from './ActiveIncidents';
import FailingAssertions from './FailingAssertions';

const FailingEntityTitle = styled(Typography.Text)`
    font-size: 16px;
    line-height: 24px;
    margin-left: 6px;
`;

const StyledWarning = styled(WarningFilled)`
    color: ${orange[5]};
    font-size: 14px;
`;

const FailingDetailsWrapper = styled.span`
    font-size: 14px;
    color: ${ANTD_GRAY[8]};
    margin-left: 6px;
    white-space: nowrap;
    &:hover {
        cursor: pointer;
        color: ${(props) => props.theme.styles['primary-color']};
    }
`;

const FailingInputsHeader = styled.div`
    display: flex;
    align-items: center;
`;

const StyledArrow = styled(DownOutlined)<{ isOpen: boolean }>`
    font-size: 12px;
    margin-left: 3px;
    ${(props) =>
        props.isOpen &&
        `
        transform: rotate(180deg);
        padding-top: 1px;
    `}
`;

interface Props {
    datasetsWithActiveIncidents: Dataset[];
    totalDatasetsWithActiveIncidents: number;
    fetchMoreIncidentsData: () => void;
    isLoadingIncidents: boolean;
    datasetsWithFailingAssertions: Dataset[];
    totalDatasetsWithFailingAssertions: number;
    fetchMoreAssertionsData: () => void;
    isLoadingAssertions: boolean;
}

export default function FailingInputs({
    datasetsWithActiveIncidents,
    totalDatasetsWithActiveIncidents,
    fetchMoreIncidentsData,
    isLoadingIncidents,
    datasetsWithFailingAssertions,
    totalDatasetsWithFailingAssertions,
    fetchMoreAssertionsData,
    isLoadingAssertions,
}: Props) {
    const [areFailingDetailsVisible, setAreFailingDetailsVisible] = useState(false);
    const entityRegistry = useEntityRegistry();
    const { entityData, entityType } = useEntityData();
    const displayedEntityType = getDisplayedEntityType(entityData, entityRegistry, entityType);

    return (
        <div>
            <FailingInputsHeader>
                <StyledWarning />
                <FailingEntityTitle ellipsis={{ tooltip: true }}>
                    Data quality issues impacting this {displayedEntityType}
                </FailingEntityTitle>
                <FailingDetailsWrapper onClick={() => setAreFailingDetailsVisible(!areFailingDetailsVisible)}>
                    details <StyledArrow isOpen={areFailingDetailsVisible} />
                </FailingDetailsWrapper>
            </FailingInputsHeader>
            {areFailingDetailsVisible && (
                <>
                    {datasetsWithActiveIncidents.length > 0 && (
                        <ActiveIncidents
                            datasetsWithActiveIncidents={datasetsWithActiveIncidents}
                            totalDatasetsWithActiveIncidents={totalDatasetsWithActiveIncidents}
                            fetchMoreIncidentsData={fetchMoreIncidentsData}
                            isLoadingIncidents={isLoadingIncidents}
                        />
                    )}
                    {datasetsWithFailingAssertions.length > 0 && (
                        <FailingAssertions
                            datasetsWithFailingAssertions={datasetsWithFailingAssertions}
                            totalDatasetsWithFailingAssertions={totalDatasetsWithFailingAssertions}
                            fetchMoreAssertionsData={fetchMoreAssertionsData}
                            isLoadingAssertions={isLoadingAssertions}
                        />
                    )}
                </>
            )}
        </div>
    );
}
