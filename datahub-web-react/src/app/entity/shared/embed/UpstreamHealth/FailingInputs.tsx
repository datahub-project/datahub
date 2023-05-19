import { orange } from '@ant-design/colors';
import { DownOutlined, WarningFilled } from '@ant-design/icons';
import React, { useState } from 'react';
import styled from 'styled-components';
import { Dataset } from '../../../../../types.generated';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { ANTD_GRAY } from '../../constants';
import { getDisplayedEntityType } from '../../containers/profile/header/utils';
import { useEntityData } from '../../EntityContext';
import FailingAssertions from './FailingAssertions';

const TextWrapper = styled.span`
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
    &:hover {
        cursor: pointer;
        color: ${(props) => props.theme.styles['primary-color']};
    }
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
    datasetsWithFailingAssertions: Dataset[];
    totalDatasetsWithFailingAssertions: number;
    fetchMoreAssertionsData: () => void;
    isLoadingAssertions: boolean;
}

export default function FailingInputs({
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
            <StyledWarning />
            <TextWrapper>Data quality issues impacting this {displayedEntityType}</TextWrapper>
            <FailingDetailsWrapper onClick={() => setAreFailingDetailsVisible(!areFailingDetailsVisible)}>
                details <StyledArrow isOpen={areFailingDetailsVisible} />
            </FailingDetailsWrapper>
            {areFailingDetailsVisible && (
                <>
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
