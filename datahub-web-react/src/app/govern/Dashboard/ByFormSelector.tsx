import { Select } from 'antd';
import React, { useEffect } from 'react';
import { useLocation } from 'react-router';
import { useFormAnalyticsContext } from './FormAnalyticsContext';
import { StyledSelect } from './components';
import { getEntityInfo, mergeRowAndHeaderData } from './utils';

export const ByFormSelector = () => {
    const {
        byForm: { forms, hasForms, selectedForm, setSelectedForm },
    } = useFormAnalyticsContext();
    const location = useLocation();

    useEffect(() => {
        const params = new URLSearchParams(location.search);
        const newForm = params.get('filter');
        if (newForm) {
            setSelectedForm(newForm);
        }

        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [location.search]);

    // If theres no forms, return null
    if (!hasForms)
        return (
            <Select options={[{ value: 'loading', label: 'Loading...' }]} size="large" style={{ width: 300 }} loading />
        );

    // format the data
    const data = mergeRowAndHeaderData(forms?.header, forms?.table || []);

    // format options
    const options = data.map((d) => {
        const entity = getEntityInfo(forms, d.form_urn);
        return {
            value: d.form_urn,
            label: entity?.info?.name || d.form_urn,
        };
    });

    // Handle figuring out the default value
    const getDefaultValue = () => {
        if (selectedForm) return selectedForm;
        if (options.length > 0) return options[0].value;
        return undefined;
    };

    return (
        <StyledSelect
            showSearch
            filterOption
            placeholder="Select a form"
            optionFilterProp="label"
            onChange={(value: any) => setSelectedForm(value)}
            options={options}
            value={getDefaultValue()}
            size="large"
            style={{ width: 300 }}
        />
    );
};
