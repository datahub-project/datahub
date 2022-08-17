import React, { useCallback, useState } from 'react';
import { Button } from 'antd';
import { useHistory } from 'react-router';
import { ArrowDownOutlined, ArrowUpOutlined, PartitionOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';

import { useEntityData } from '../../EntityContext';
import TabToolbar from '../../components/styled/TabToolbar';
import { getEntityPath } from '../../containers/profile/utils';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { ImpactAnalysis } from './ImpactAnalysis';
import { LineageDirection } from '../../../../../types.generated';

const StyledTabToolbar = styled(TabToolbar)`
    justify-content: space-between;
`;

const StyledButton = styled(Button)<{ isSelected: boolean }>`
    ${(props) =>
        props.isSelected &&
        `
        color: #1890ff;
        &:focus {
            color: #1890ff;
        }    
    `}
`;

export const LineageTab = () => {
    const { urn, entityType } = useEntityData();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const [lineageDirection, setLineageDirection] = useState<string>(LineageDirection.Downstream);

    const routeToLineage = useCallback(() => {
        history.push(getEntityPath(entityType, urn, entityRegistry, true, false));
    }, [history, entityType, urn, entityRegistry]);

    return (
        <>
            <StyledTabToolbar>
                <div>
                    <StyledButton
                        type="text"
                        isSelected={lineageDirection === LineageDirection.Downstream}
                        onClick={() => setLineageDirection(LineageDirection.Downstream)}
                    >
                        <ArrowDownOutlined /> Downstream
                    </StyledButton>
                    <StyledButton
                        type="text"
                        isSelected={lineageDirection === LineageDirection.Upstream}
                        onClick={() => setLineageDirection(LineageDirection.Upstream)}
                    >
                        <ArrowUpOutlined /> Upstream
                    </StyledButton>
                </div>
                <Button type="text" onClick={routeToLineage}>
                    <PartitionOutlined />
                    Visualize Lineage
                </Button>
            </StyledTabToolbar>
            <ImpactAnalysis urn={urn} direction={lineageDirection as LineageDirection} />
        </>
    );
};
