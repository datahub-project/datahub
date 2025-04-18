import { Icon, Text, Tooltip } from '@components';
import { AllowedValue } from '@src/types.generated';
import React from 'react';
import {
    FieldLabel,
    FlexContainer,
    ItemsContainer,
    RowContainer,
    StyledIcon,
    ValueListContainer,
    ValuesList,
    ValueType,
    VerticalDivider,
} from './styledComponents';
import { isStringOrNumberTypeSelected, PropValueField } from './utils';

interface Props {
    selectedValueType: string;
    allowedValues: AllowedValue[] | undefined;
    valueField: PropValueField;
    setShowAllowedValuesDrawer: React.Dispatch<React.SetStateAction<boolean>>;
}

const AllowedValuesField = ({ selectedValueType, allowedValues, valueField, setShowAllowedValuesDrawer }: Props) => {
    return (
        <>
            {isStringOrNumberTypeSelected(selectedValueType) && (
                <RowContainer>
                    <FieldLabel>
                        <FlexContainer>
                            Allowed Values
                            <Tooltip
                                title="Define the set of valid values for this property. If none are provided, any value will be allowed"
                                showArrow={false}
                            >
                                <Icon icon="Info" color="violet" size="lg" />
                            </Tooltip>
                        </FlexContainer>
                    </FieldLabel>

                    {allowedValues && allowedValues.length > 0 ? (
                        <ItemsContainer>
                            <ValuesList>
                                {allowedValues.map((val, index) => {
                                    return (
                                        <>
                                            <Text>{val[valueField]}</Text>
                                            {index < allowedValues.length - 1 && <VerticalDivider type="vertical" />}
                                        </>
                                    );
                                })}
                            </ValuesList>
                            <Tooltip title="Update allowed values" showArrow={false}>
                                <StyledIcon
                                    icon="ChevronRight"
                                    color="gray"
                                    onClick={() => setShowAllowedValuesDrawer(true)}
                                />
                            </Tooltip>
                        </ItemsContainer>
                    ) : (
                        <ValueListContainer>
                            Any
                            <ValueType>{valueField === 'stringValue' ? 'text' : 'number'} </ValueType>
                            value will be allowed
                            <Tooltip title="Update allowed values" showArrow={false}>
                                <Icon icon="Add" color="gray" onClick={() => setShowAllowedValuesDrawer(true)} />
                            </Tooltip>
                        </ValueListContainer>
                    )}
                </RowContainer>
            )}
        </>
    );
};

export default AllowedValuesField;
