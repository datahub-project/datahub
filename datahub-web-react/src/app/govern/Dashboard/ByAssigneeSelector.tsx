import React from 'react';

import { Select } from 'antd';
import analytics, { EventType } from '@src/app/analytics';

import { mergeRowAndHeaderData, getEntityInfo } from './utils';

import { useFormAnalyticsContext } from './FormAnalyticsContext';
import { StyledSelect } from './components';

export const ByAssigneeSelector = () => {
    const {
        tabs: { selectedTab },
        byAssignee: { assignees, hasAssignees, selectedAssignee, setSelectedAssignee },
    } = useFormAnalyticsContext();

    // If theres no forms, return null
    if (!hasAssignees)
        return (
            <Select options={[{ value: 'loading', label: 'Loading...' }]} size="large" style={{ width: 300 }} loading />
        );

    // format the data
    const data = mergeRowAndHeaderData(assignees?.header, assignees?.table || []);

    // format options
    const options = data.map((d) => {
        const { properties, groupInfo } = getEntityInfo(assignees, d.assignee_urn) || {};
        const displayName = properties?.displayName || groupInfo?.displayName || d.assignee_urn;
        return {
            value: d.assignee_urn,
            label: displayName,
        };
    });

    // Handle figuring out the default value
    const getDefaultValue = () => {
        if (selectedAssignee) return selectedAssignee;
        if (options.length > 0) return options[0].value;
        return undefined;
    };

    function handleChange(value: any) {
        analytics.event({ type: EventType.FormAnalyticsTabFilter, selectedTab });
        setSelectedAssignee(value);
    }

    return (
        <StyledSelect
            showSearch
            filterOption
            placeholder="Select an assignee"
            optionFilterProp="label"
            onChange={handleChange}
            options={options}
            defaultValue={getDefaultValue()}
            size="large"
            style={{ width: 300 }}
        />
    );
};
