import React from 'react';

import { Select } from 'antd';

import { mergeRowAndHeaderData, getEntityInfo } from './utils';

import { useFormAnalyticsContext } from './FormAnalyticsContext';

import { NO_DOMAIN } from '../../../conf/Global';
import { StyledSelect } from './components';

export const ByDomainSelector = () => {
    const {
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

    return (
        <StyledSelect
            showSearch
            filterOption
            placeholder="Select a domain"
            optionFilterProp="label"
            onChange={(value: any) => setSelectedDomain(value)}
            options={options}
            defaultValue={getDefaultValue()}
            size="large"
            style={{ width: 300 }}
        />
    );
};
