import { Select } from 'antd';
import React from 'react';

import { useFormAnalyticsContext } from '@app/govern/Dashboard/FormAnalyticsContext';
import { StyledSelect } from '@app/govern/Dashboard/components';
import { getEntityInfo, mergeRowAndHeaderData } from '@app/govern/Dashboard/utils';
import { NO_DOMAIN } from '@conf/Global';
import analytics, { EventType } from '@src/app/analytics';

export const ByDomainSelector = () => {
    const {
        tabs: { selectedTab },
        byDomain: { domains, hasDomains, selectedDomain, setSelectedDomain },
    } = useFormAnalyticsContext();

    // If theres no forms, return null
    if (!hasDomains)
        return (
            <Select options={[{ value: 'loading', label: 'Loading...' }]} size="large" style={{ width: 300 }} loading />
        );

    // format the data
    const data = mergeRowAndHeaderData(domains?.header, domains?.table || []);

    // format options
    const options = [
        {
            value: 'null',
            label: NO_DOMAIN,
        },
    ];

    data.forEach((d) => {
        const name = getEntityInfo(domains, d.domain_urn)?.properties?.name || d.domain_urn || NO_DOMAIN;
        if (name !== NO_DOMAIN) {
            options.push({
                value: d.domain_urn,
                label: name,
            });
        }
    });

    // Handle figuring out the default value
    const getDefaultValue = () => {
        if (selectedDomain) return selectedDomain;
        if (options.length > 0) return options[0].value;
        return undefined;
    };

    function handleChange(value: any) {
        analytics.event({ type: EventType.FormAnalyticsTabFilter, selectedTab });
        setSelectedDomain(value);
    }

    return (
        <StyledSelect
            showSearch
            filterOption
            placeholder="Select a domain"
            optionFilterProp="label"
            onChange={handleChange}
            options={options}
            defaultValue={getDefaultValue()}
            size="large"
            style={{ width: 300 }}
        />
    );
};
