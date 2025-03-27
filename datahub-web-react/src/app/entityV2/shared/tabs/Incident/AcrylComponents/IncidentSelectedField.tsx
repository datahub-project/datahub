import React from 'react';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { IncidentPriorityLabel } from '@src/alchemy-components/components/IncidentPriorityLabel';
import { IncidentStagePill } from '@src/alchemy-components/components/IncidentStagePill';
import { colors, SimpleSelect } from '@src/alchemy-components';
import { IncidentState } from '@src/types.generated';
import { Check, Warning } from '@phosphor-icons/react';
import { IconLabel } from '@src/alchemy-components/components/IconLabel';
import { getCapitalizeWord } from '@src/alchemy-components/components/IncidentStagePill/utils';
import { IconType } from '@src/alchemy-components/components/IconLabel/types';
import { FormInstance } from 'antd/es/form/Form';

import { INCIDENT_OPTION_LABEL_MAPPING } from '../constant';
import { SelectFormItem, SelectWrapper } from './styledComponents';
import { IncidentConstant } from '../types';

const IncidentStates = {
    [IncidentState.Active]: {
        label: IncidentState.Active,
        icon: <Warning color={colors.red[1200]} width={17} height={15} />,
    },
    [IncidentState.Resolved]: {
        label: IncidentState.Resolved,
        icon: <Check color={colors.green[1200]} width={17} height={15} />,
    },
};

interface IncidentSelectFieldProps {
    incidentLabelMap: {
        label: string;
        name: string;
        fieldName: string;
    };
    options: Array<any>;
    onUpdate?: (value: string) => void;
    form: FormInstance;
    handleValuesChange: (values: any) => void;
    showClear?: boolean;
    width?: string;
    customStyle?: React.CSSProperties;
    isDisabled?: boolean;
    value?: string;
}

export const IncidentSelectField = ({
    incidentLabelMap,
    options,
    onUpdate,
    form,
    handleValuesChange,
    showClear = true,
    width,
    customStyle,
    isDisabled,
    value,
}: IncidentSelectFieldProps) => {
    const { label, name } = incidentLabelMap;
    const placeholder = label.toLowerCase() === IncidentConstant.PRIORITY ? 'priority level' : label.toLowerCase();
    const isRequiredField = label.toLowerCase() === IncidentConstant.CATEGORY;
    const renderOption = (option: SelectOption) => {
        switch (label) {
            case INCIDENT_OPTION_LABEL_MAPPING.category.label:
                return option.label;
            case INCIDENT_OPTION_LABEL_MAPPING.state.label:
                return (
                    <IconLabel
                        name={getCapitalizeWord(IncidentStates[option.value]?.label)}
                        icon={IncidentStates[option.value]?.icon}
                        type={IconType.ICON}
                    />
                );
            case INCIDENT_OPTION_LABEL_MAPPING.priority.label:
                return (
                    <IncidentPriorityLabel
                        style={{
                            width: '22px',
                            justifyContent: 'center',
                        }}
                        priority={option.value}
                        title={option.label}
                    />
                );
            case INCIDENT_OPTION_LABEL_MAPPING.stage.label:
                return <IncidentStagePill stage={option.value} />;
            default:
                return null;
        }
    };

    const renderSelectedValue = (selectedOption: SelectOption) => {
        if (!selectedOption) {
            return null;
        }
        switch (label) {
            case INCIDENT_OPTION_LABEL_MAPPING.category.label:
                return selectedOption?.label;
            case INCIDENT_OPTION_LABEL_MAPPING.state.label:
                return (
                    <IconLabel
                        name={getCapitalizeWord(IncidentStates[selectedOption?.value]?.label)}
                        icon={IncidentStates[selectedOption?.value]?.icon}
                        type={IconType.ICON}
                    />
                );
            case INCIDENT_OPTION_LABEL_MAPPING.priority.label:
                return (
                    <IncidentPriorityLabel
                        style={{
                            width: 'auto',
                        }}
                        priority={selectedOption?.value}
                        title={selectedOption?.label}
                    />
                );
            case INCIDENT_OPTION_LABEL_MAPPING.stage.label:
                return <IncidentStagePill stage={selectedOption?.value} />;
            default:
                return null;
        }
    };

    return (
        <SelectFormItem
            label={label}
            name={name}
            rules={[{ required: isRequiredField, message: `Please select ${label.toLowerCase()}!` }]}
            initialValue={value}
            customStyle={customStyle}
        >
            <SelectWrapper style={{ width: width ?? '50%' }}>
                <SimpleSelect
                    options={options}
                    values={value ? [value] : []}
                    onUpdate={(selectedValues: string[]) => {
                        onUpdate?.(selectedValues[0]);
                        form.setFieldsValue({ ...form.getFieldsValue(), [name]: selectedValues[0] });
                        handleValuesChange(form.getFieldsValue());
                    }}
                    placeholder={`Select ${placeholder}`}
                    size="md"
                    showClear={showClear}
                    width={10}
                    renderCustomOptionText={renderOption}
                    renderCustomSelectedValue={renderSelectedValue}
                    selectLabelProps={{ variant: 'custom' }}
                    position="start"
                    optionListTestId={`${label?.toLocaleLowerCase()}-options-list`}
                    data-testid={`${label?.toLocaleLowerCase()}-select-input-type`}
                    isDisabled={isDisabled}
                />
            </SelectWrapper>
        </SelectFormItem>
    );
};
