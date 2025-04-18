import { Form } from 'antd';
import { Tooltip } from '@components';
import { CheckboxChangeEvent } from 'antd/lib/checkbox';
import React, { useState } from 'react';
import { AllowedItemsWrapper, StyledCheckbox, StyledLabel } from '../styledComponents';
import CardinalityField from './CardinalityField';
import OwnershipSelector from './OwnershipSelector';
import OwnershipTypeSelector from './OwnershipTypeSelector';

const OwnershipQuestion = () => {
    const form = Form.useFormInstance();
    const allowedOwners = form.getFieldValue(['ownershipParams', 'allowedOwners']);
    const [anyOwnersSelected, setAnyOwnersSelected] = useState(allowedOwners ? !allowedOwners.length : true);
    const allowedOwnershipTypes = form.getFieldValue(['ownershipParams', 'allowedOwnershipTypes']);
    const [anyOwnershipTypeSelected, setAnyOwnershipTypeSelected] = useState(
        allowedOwnershipTypes ? !allowedOwnershipTypes.length : true,
    );

    function handleAllowedOwnersChange(e: CheckboxChangeEvent) {
        const allowAnyOwner = !e.target.checked;
        setAnyOwnersSelected(allowAnyOwner);
        if (allowAnyOwner) {
            form.setFieldValue(['ownershipParams', 'allowedOwners'], undefined);
        }
    }

    function handleAllowedOwnershipTypesChange(e: CheckboxChangeEvent) {
        const allowAnyOwnershipType = !e.target.checked;
        setAnyOwnershipTypeSelected(allowAnyOwnershipType);
        if (allowAnyOwnershipType) {
            form.setFieldValue(['ownershipParams', 'allowedOwnershipTypes'], undefined);
        }
    }

    return (
        <>
            <CardinalityField paramsField="ownershipParams" inputType="owners" />
            <AllowedItemsWrapper>
                <StyledCheckbox checked={!anyOwnersSelected} onChange={handleAllowedOwnersChange} />
                <Tooltip title="If left unchecked, then any owner will be allowed" showArrow={false}>
                    <StyledLabel>Restrict responses to specific owners</StyledLabel>
                </Tooltip>
                {!anyOwnersSelected && <OwnershipSelector />}
            </AllowedItemsWrapper>
            <AllowedItemsWrapper>
                <StyledCheckbox checked={!anyOwnershipTypeSelected} onChange={handleAllowedOwnershipTypesChange} />
                <Tooltip title="If left unchecked, then any owner type will be allowed" showArrow={false}>
                    <StyledLabel>Restrict responses to specific owner types</StyledLabel>
                </Tooltip>
                {!anyOwnershipTypeSelected && <OwnershipTypeSelector />}
            </AllowedItemsWrapper>
        </>
    );
};

export default OwnershipQuestion;
