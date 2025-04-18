import { NestedSelectOption } from '@components/components/Select/Nested/types';
import React, { useState, useEffect } from 'react';
import { Radio } from 'antd';
import styled from 'styled-components';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { sharedStyles } from '@app/automations/sharedComponents';
import GlossaryTermsSelector from '@src/app/govern/Dashboard/Forms/questionTypes/GlossaryTermsSelector';
import { useGetEntitiesLazyQuery } from '@src/graphql/entity.generated';
import { isResolutionRequired } from '@src/app/entityV2/view/builder/utils';
import { LoadingOutlined } from '@ant-design/icons';
import { EntityType } from '@src/types.generated';
import type { SelectDropdownProps, RadioValue } from './types';
import { SelectDropdown } from './SelectDropdown';
import { useGlossaryOptionsBuilder } from '../hooks';
import { getEntitiesByTagORTerm } from './utils';

const Wrapper = styled.div`
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 28px;
    border: 1px solid ${sharedStyles.borderColor};
    border-radius: ${sharedStyles.borderRadius};
    padding: 16px;

    &.termOptionSingleContainer {
        gap: 0px;
        padding: 0px;
        width: 100%;
        display: block;
        label {
            display: flex !important;
        }
    }
`;

const DropdownsWrapper = styled.div`
    display: grid;
    gap: 16px;
    width: 100%;
`;

const ContentWrapper = styled.div`
    display: flex;
    background-color: rgba(0, 0, 0, 0.03);
    border-radius: ${sharedStyles.borderRadius};
    padding: 16px;
    align-items: center;
    justify-content: center;

    &.termSelector {
        padding: 0px;
        border: none;
    }
`;

const StyledRadioGroup = styled(Radio.Group)`
    display: flex;
    flex-direction: column;
    gap: 0;

    .ant-radio-wrapper {
        margin-bottom: 2px;
        font-weight: 400 !important;
    }
`;

export const Label = styled.div`
    font-size: 12px;
    font-weight: 600;
    margin-bottom: 4px;

    &.heading {
        font-size: 14px;
    }
`;

type Props = {
    type: EntityType;
    typeName: string;
    selects: SelectDropdownProps[];
    radio: {
        allowedRadios: string[]; // Allowed radio button options (e.g., "all", "some", "none")
        preselectedValue: RadioValue; // Default radio value
    };
    onChange: (value: any, entities: EntityType[]) => void;
    isEdit?: boolean;
    shouldUseGlossaryTermComponent?: boolean;
};

// Type for selected options
type SelectedOptionType = {
    GLOSSARY_NODE: string[]; // Selected glossary nodes
    GLOSSARY_TERM: string[]; // Selected glossary terms
};

const checkShouldRenderTermsSelector = ({
    isEdit,
    resolvedEntitiesData,
    initialOptions,
}: {
    isEdit?: boolean;
    resolvedEntitiesData: any[];
    initialOptions: any[];
}) => {
    if (!isEdit) return true; // If not in edit mode, return true (show selector)
    if (resolvedEntitiesData && initialOptions.length) return true; // Show if entities are resolved and options are ready
    return false; // Otherwise, hide
};

const GlossaryTermDropdownSwitcher = ({
    shouldUseGlossaryTermComponent,
    loading,
    onUpdate,
    initialOptions,
    selects,
    handleTermsChange,
    isEdit,
    resolvedEntitiesData,
}: {
    shouldUseGlossaryTermComponent?: boolean;
    loading: boolean;
    onUpdate: (values: NestedSelectOption[]) => void;
    initialOptions: any[];
    selects: SelectDropdownProps[]; // Dropdown options for selection
    handleTermsChange: (values: any, entity: EntityType) => void;
    isEdit?: boolean;
    resolvedEntitiesData: any;
}): JSX.Element => {
    if (shouldUseGlossaryTermComponent) {
        const shouldRenderTermsSelector = checkShouldRenderTermsSelector({
            isEdit,
            resolvedEntitiesData,
            initialOptions,
        });
        if (loading) {
            // If loading, show loading spinner
            return <LoadingOutlined style={{ fontSize: 24, margin: 20 }} spin />;
        }
        if (shouldRenderTermsSelector) {
            return <GlossaryTermsSelector onUpdate={onUpdate} initialOptions={initialOptions} areNodeSelectable />;
        } // Render glossary selector if conditions are met
        return <></>;
    }
    return (
        <>
            {selects.map((select) => (
                <SelectDropdown
                    key={select.type}
                    placeholder={`Select ${select.label}...`}
                    onChange={(value) => handleTermsChange(value, select.type)}
                    {...select}
                />
            ))}
        </>
    );
};

// Main TermOption component
export const TermOption = ({
    type,
    typeName,
    selects,
    radio,
    onChange,
    isEdit,
    shouldUseGlossaryTermComponent,
}: Props) => {
    const { allowedRadios, preselectedValue } = radio;
    const [isUrnDataFetched, setIsUrnDataFetched] = useState<boolean>(false); // Flag to track URN data fetching
    const [selectedOptions, setSelectedOptions] = useState<SelectedOptionType>(() => {
        // State to track selected options
        const initialSelected = {}; // Initialize selection
        selects.forEach((select) => {
            initialSelected[select.type] = select.preselectedOptions || []; // Set preselected options
        });
        return initialSelected as SelectedOptionType; // Return initial selected options
    });
    const [urnsToFetch, setUrnsToFetch] = useState<string[]>([]); // List of URNs to fetch
    const [radioValue, setRadioValue] = useState<RadioValue>(preselectedValue); // Track current radio button value

    const [getEntities, { data: resolvedEntitiesData, loading }] = useGetEntitiesLazyQuery(); // Lazy query to fetch entities

    const { entityCache, initialOptions } = useGlossaryOptionsBuilder(
        resolvedEntitiesData,
        urnsToFetch,
        setUrnsToFetch,
    );

    useEffect(() => {
        if (isResolutionRequired(urnsToFetch, entityCache)) {
            // Check if more URNs need to be fetched
            getEntities({ variables: { urns: urnsToFetch } }); // Fetch entities based on URNs
        }
    }, [urnsToFetch, entityCache, getEntities]);

    // Effect to fetch URNs if in edit mode and data has not been fetched yet
    useEffect(() => {
        const urns = selects.flatMap((select) => select.preselectedOptions || []); // Get preselected options' URNs
        if (isEdit && !isUrnDataFetched && urns.length) {
            // If editing and URNs haven't been fetched
            setIsUrnDataFetched(true);
            setUrnsToFetch(urns); // Set URNs to fetch
        }
    }, [selects, isEdit, isUrnDataFetched]);

    // Handle change in terms selection
    const handleTermsChange = (values: any, entity: EntityType) => {
        const newSelectedOptions = { ...selectedOptions, ...values };
        setSelectedOptions(newSelectedOptions);
        onChange({ selectionType: radioValue, selected: newSelectedOptions }, getEntitiesByTagORTerm(entity));
    };

    const handleRadioChange = (value: RadioValue) => {
        setRadioValue(value);
        onChange(
            {
                selectionType: value,
                selected: selectedOptions,
            },
            getEntitiesByTagORTerm(type),
        );
    };

    // Effect to reset radio value when preselectedValue changes
    useEffect(() => {
        setRadioValue(preselectedValue); // Reset radio value to preselected value
    }, [preselectedValue]);

    // Handle updates from glossary terms selector
    const onUpdate = (values: NestedSelectOption[]) => {
        const newSelectedOptions = values.reduce<SelectedOptionType>(
            (acc, glossary) => {
                const { isParent, value } = glossary;
                if (isParent) acc.GLOSSARY_NODE.push(value); // If parent node, add to GLOSSARY_NODE
                else acc.GLOSSARY_TERM.push(value); // If glossary term, add to GLOSSARY_TERM
                return acc;
            },
            { GLOSSARY_NODE: [], GLOSSARY_TERM: [] }, // Initialize selection types
        );
        setSelectedOptions(newSelectedOptions);
        onChange(
            {
                selectionType: radioValue,
                selected: newSelectedOptions,
            },
            getEntitiesByTagORTerm(type),
        );
    };

    // Build radio button options
    const options = allowedRadios.map((item) => {
        let label;
        if (item === 'all') {
            label = `All ${typeName}`;
        } else if (item === 'none') {
            label = 'None';
        } else {
            label = capitalizeFirstLetterOnly(`${typeName} in a specific set`);
        }

        return {
            label,
            value: item,
        };
    });

    return (
        <Wrapper className={shouldUseGlossaryTermComponent ? 'termOptionSingleContainer' : undefined}>
            {!shouldUseGlossaryTermComponent && (
                <div>
                    <Label>Allowed {typeName}</Label>
                    <StyledRadioGroup
                        options={options}
                        value={radioValue}
                        onChange={(e) => handleRadioChange(e.target.value)}
                    />
                </div>
            )}

            <ContentWrapper className={shouldUseGlossaryTermComponent ? 'termSelector' : undefined}>
                {radioValue === 'all' && (
                    <Label className="heading">All {typeName.toLowerCase()} will be propagated.</Label>
                )}
                {radioValue === 'some' && (
                    <DropdownsWrapper>
                        <GlossaryTermDropdownSwitcher
                            handleTermsChange={handleTermsChange}
                            initialOptions={initialOptions}
                            isEdit={isEdit}
                            loading={loading}
                            onUpdate={onUpdate}
                            resolvedEntitiesData={resolvedEntitiesData}
                            selects={selects}
                            shouldUseGlossaryTermComponent={shouldUseGlossaryTermComponent}
                        />
                    </DropdownsWrapper>
                )}
                {radioValue === 'none' && (
                    <Label className="heading">No {typeName.toLowerCase()} will be propagated.</Label>
                )}
            </ContentWrapper>
        </Wrapper>
    );
};
