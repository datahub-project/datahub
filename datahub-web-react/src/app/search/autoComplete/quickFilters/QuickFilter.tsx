import { Button } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { useAppStateContext } from '../../../../providers/AppStateContext';
import { QuickFilter } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { getQuickFilterDetails } from './utils';
import { ANTD_GRAY } from '../../../entity/shared/constants';

const QuickFilterWrapper = styled(Button)<{ isSelected: boolean }>`
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 16px;
    box-shadow: none;
    font-weight: 400;
    color: black;
    cursor: pointer;
    display: flex;
    align-items: center;
    padding: 2px 10px;
    font-size: 12px;
    margin: 4px;

    &:hover {
        color: black;
    }

    ${(props) =>
        props.isSelected &&
        `
        // TODO - make this light color work with acryl and oss
        // background-color: ${props.theme.styles['primary-color']};
        background-color: rgba(24, 144, 255, 0.15)};
        &:hover {
            background-color: rgba(24, 144, 255, 0.15)};
        }
    `}
`;

const LabelWrapper = styled.span`
    margin-left: 4px;
`;

interface Props {
    quickFilter: QuickFilter;
}

export default function QuickFilter({ quickFilter }: Props) {
    const entityRegistry = useEntityRegistry();
    const { selectedQuickFilter, setSelectedQuickFilter } = useAppStateContext();

    const isSelected = selectedQuickFilter?.value === quickFilter.value;
    const { label, icon } = getQuickFilterDetails(quickFilter, entityRegistry);

    function handleClick() {
        if (isSelected) {
            setSelectedQuickFilter(null);
        } else {
            setSelectedQuickFilter(quickFilter);
        }
    }

    return (
        <QuickFilterWrapper onClick={handleClick} isSelected={isSelected}>
            {icon}
            <LabelWrapper>{label}</LabelWrapper>
        </QuickFilterWrapper>
    );
}
