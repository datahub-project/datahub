import React, { useCallback, useState } from 'react';
import { Button } from 'antd';
import * as QueryString from 'query-string';
import { useHistory, useLocation } from 'react-router';
import { ArrowDownOutlined, ArrowUpOutlined, PartitionOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';

import { useEntityData } from '../../EntityContext';
import TabToolbar from '../../components/styled/TabToolbar';
import { getEntityPath } from '../../containers/profile/utils';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { ImpactAnalysis } from './ImpactAnalysis';
import { LineageDirection } from '../../../../../types.generated';
import { generateSchemaFieldUrn } from './utils';
import { downgradeV2FieldPath } from '../../../dataset/profile/schema/utils/utils';
import ColumnsLineageSelect from './ColumnLineageSelect';
import { LineageTabContext } from './LineageTabContext';

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

const RightButtonsWrapper = styled.div`
    align-items: center;
    display: flex;
`;

export const LineageTab = ({
    properties = { defaultDirection: LineageDirection.Downstream },
}: {
    properties?: { defaultDirection: LineageDirection };
}) => {
    const { urn, entityType } = useEntityData();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const location = useLocation();
    const params = QueryString.parse(location.search, { arrayFormat: 'comma' });
    const [lineageDirection, setLineageDirection] = useState<LineageDirection>(properties.defaultDirection);
    const [selectedColumn, setSelectedColumn] = useState<string | undefined>(params?.column as string);
    const [isColumnLevelLineage, setIsColumnLevelLineage] = useState(!!params?.column);

    const routeToLineage = useCallback(() => {
        history.push(getEntityPath(entityType, urn, entityRegistry, true, false));
    }, [history, entityType, urn, entityRegistry]);

    const selectedV1FieldPath = downgradeV2FieldPath(selectedColumn) || '';
    const selectedColumnUrn = generateSchemaFieldUrn(selectedV1FieldPath, urn);
    const impactAnalysisUrn = isColumnLevelLineage && selectedColumnUrn ? selectedColumnUrn : urn;

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
                <RightButtonsWrapper>
                    <ColumnsLineageSelect
                        selectedColumn={selectedColumn}
                        isColumnLevelLineage={isColumnLevelLineage}
                        setSelectedColumn={setSelectedColumn}
                        setIsColumnLevelLineage={setIsColumnLevelLineage}
                    />
                    <Button type="text" onClick={routeToLineage}>
                        <PartitionOutlined />
                        Visualize Lineage
                    </Button>
                </RightButtonsWrapper>
            </StyledTabToolbar>
            <LineageTabContext.Provider value={{ isColumnLevelLineage, selectedColumn, lineageDirection }}>
                <ImpactAnalysis urn={impactAnalysisUrn} direction={lineageDirection as LineageDirection} />
            </LineageTabContext.Provider>
        </>
    );
};
