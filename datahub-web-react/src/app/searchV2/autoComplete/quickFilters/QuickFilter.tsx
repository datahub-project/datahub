/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Button } from 'antd';
import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import analytics, { Event, EventType } from '@app/analytics';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { getQuickFilterDetails } from '@app/searchV2/autoComplete/quickFilters/utils';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useQuickFiltersContext } from '@providers/QuickFiltersContext';

import { QuickFilter as QuickFilterType } from '@types';

const QuickFilterWrapper = styled(Button)<{ selected: boolean }>`
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 16px;
    box-shadow: none;
    font-weight: 400;
    color: black;
    cursor: pointer;
    display: flex;
    align-items: center;
    padding: 2px 10px;
    font-size: 14px;
    margin: 4px;

    &:hover {
        color: black;
    }

    ${(props) =>
        props.selected &&
        `
        border: 1px solid ${props.theme.styles['primary-color-dark']};
        background-color: ${props.theme.styles['primary-color-light']};
        &:hover {
            background-color: ${props.theme.styles['primary-color-light']};
        }
    `}
`;

const LabelWrapper = styled.span`
    margin-left: 4px;
`;

interface Props {
    quickFilter: QuickFilterType;
    searchQuery?: string;
    setIsDropdownVisible: React.Dispatch<React.SetStateAction<boolean>>;
}

export default function QuickFilter({ quickFilter, searchQuery, setIsDropdownVisible }: Props) {
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const { selectedQuickFilter, setSelectedQuickFilter } = useQuickFiltersContext();

    const isSelected = selectedQuickFilter?.value === quickFilter.value;
    const { label, icon } = getQuickFilterDetails(quickFilter, entityRegistry);

    function emitTrackingEvent(isSelecting: boolean) {
        analytics.event({
            type: isSelecting ? EventType.SelectQuickFilterEvent : EventType.DeselectQuickFilterEvent,
            quickFilterType: quickFilter.field,
            quickFilterValue: quickFilter.value,
        } as Event);
    }

    function handleClick() {
        if (isSelected) {
            setSelectedQuickFilter(null);
            emitTrackingEvent(false);
        } else {
            setSelectedQuickFilter(quickFilter);
            emitTrackingEvent(true);

            navigateToSearchUrl({
                query: searchQuery || '*',
                filters: [
                    {
                        field: quickFilter.field,
                        values: [quickFilter.value],
                    },
                ],
                history,
            });
            setIsDropdownVisible(false);
        }
    }

    return (
        <QuickFilterWrapper
            onClick={handleClick}
            selected={isSelected}
            data-testid={`quick-filter-${quickFilter.value}`}
        >
            {icon}
            <LabelWrapper>{label}</LabelWrapper>
        </QuickFilterWrapper>
    );
}
