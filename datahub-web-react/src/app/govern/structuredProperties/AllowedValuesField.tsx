import { Icon, Text } from '@components';
import { AllowedValue, SearchResult, StructuredPropertyEntity } from '@src/types.generated';
import { Tooltip } from 'antd';
import React, { useState } from 'react';
import AllowedValuesModal from './AllowedValuesModal';
import {
    FieldLabel,
    FlexContainer,
    ItemsContainer,
    RowContainer,
    StyledIcon,
    ValueListContainer,
    ValuesList,
    VerticalDivider,
} from './styledComponents';
import { isStringOrNumberTypeSelected, PropValueField } from './utils';

interface Props {
    selectedProperty: SearchResult | undefined;
    isEditMode: boolean;
    selectedValueType: string;
    allowedValues: AllowedValue[] | undefined;
    setAllowedValues: React.Dispatch<React.SetStateAction<AllowedValue[] | undefined>>;
    valueField: PropValueField;
}

const AllowedValuesField = ({
    selectedProperty,
    isEditMode,
    selectedValueType,
    allowedValues,
    setAllowedValues,
    valueField,
}: Props) => {
    const [showAllowedValuesModal, setShowAllowedValuesModal] = useState<boolean>(false);

    return (
        <>
            {isStringOrNumberTypeSelected(selectedValueType) && (
                <RowContainer>
                    <FieldLabel>
                        <FlexContainer>
                            Allowed Values
                            <Tooltip
                                title="Define a set of valid values that can be set on an asset with this structured property. The user will be prompted to pick from this list when applying this structured property to an asset."
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
                            <StyledIcon
                                icon="ChevronRight"
                                color="gray"
                                onClick={() => setShowAllowedValuesModal(true)}
                            />
                        </ItemsContainer>
                    ) : (
                        <ValueListContainer>
                            <Tooltip title="Add new allowed values" showArrow={false}>
                                <Icon icon="Add" color="gray" onClick={() => setShowAllowedValuesModal(true)} />
                            </Tooltip>
                        </ValueListContainer>
                    )}
                </RowContainer>
            )}
            <AllowedValuesModal
                isOpen={showAllowedValuesModal}
                showAllowedValuesModal={showAllowedValuesModal}
                setShowAllowedValuesModal={setShowAllowedValuesModal}
                propType={valueField}
                allowedValues={allowedValues}
                setAllowedValues={setAllowedValues}
                isEditMode={isEditMode}
                noOfExistingValues={
                    (selectedProperty?.entity as StructuredPropertyEntity)?.definition?.allowedValues?.length || 0
                }
            />
        </>
    );
};

export default AllowedValuesField;
