/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Icon, Text, Tooltip } from '@components';
import React from 'react';

import {
    FieldLabel,
    FlexContainer,
    ItemsContainer,
    RowContainer,
    StyledIcon,
    ValueListContainer,
    ValueType,
    ValuesList,
    VerticalDivider,
} from '@app/govern/structuredProperties/styledComponents';
import { PropValueField, isStringOrNumberTypeSelected } from '@app/govern/structuredProperties/utils';
import { AllowedValue } from '@src/types.generated';

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
