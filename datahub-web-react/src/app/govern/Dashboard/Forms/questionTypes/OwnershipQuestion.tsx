import { Form, Radio, RadioChangeEvent, Space } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FieldLabel, StyledRadioGroup } from '../styledComponents';
import CardinalityField from './CardinalityField';
import OwnershipSelector from './OwnershipSelector';
import OwnershipTypeSelector from './OwnershipTypeSelector';

const AllowedTermsWrapper = styled.div`
    margin-bottom: 24px;
`;

const OwnershipQuestion = () => {
    const form = Form.useFormInstance();
    const allowedOwners = form.getFieldValue(['ownershipParams', 'allowedOwners']);
    const [anyOwnersSelected, setAnyOwnersSelected] = useState(allowedOwners ? !allowedOwners.length : true);
    const allowedOwnershipTypes = form.getFieldValue(['ownershipParams', 'allowedOwnershipTypes']);
    const [anyOwnershipTypeSelected, setAnyOwnershipTypeSelected] = useState(
        allowedOwnershipTypes ? !allowedOwnershipTypes.length : true,
    );

    function handleAllowedOwnersChange(e: RadioChangeEvent) {
        const allowAnyOwner = e.target.value;
        setAnyOwnersSelected(allowAnyOwner);
        if (allowAnyOwner) {
            form.setFieldValue(['ownershipParams', 'allowedOwners'], undefined);
        }
    }

    function handleAllowedOwnershipTypesChange(e: RadioChangeEvent) {
        const allowAnyOwnershipType = e.target.value;
        setAnyOwnershipTypeSelected(allowAnyOwnershipType);
        if (allowAnyOwnershipType) {
            form.setFieldValue(['ownershipParams', 'allowedOwnershipTypes'], undefined);
        }
    }

    return (
        <>
            <AllowedTermsWrapper>
                <FieldLabel>Allowed Owners</FieldLabel>
                <StyledRadioGroup value={anyOwnersSelected} onChange={handleAllowedOwnersChange}>
                    <Space direction="vertical">
                        <Radio value>Any owners</Radio>
                        <Radio value={false}>Specific users or user groups</Radio>
                    </Space>
                </StyledRadioGroup>
                {!anyOwnersSelected && <OwnershipSelector />}
            </AllowedTermsWrapper>
            <AllowedTermsWrapper>
                <FieldLabel>Allowed Ownership Types</FieldLabel>
                <StyledRadioGroup value={anyOwnershipTypeSelected} onChange={handleAllowedOwnershipTypesChange}>
                    <Space direction="vertical">
                        <Radio value>Any ownership types</Radio>
                        <Radio value={false}>Specific ownership types</Radio>
                    </Space>
                </StyledRadioGroup>
                {!anyOwnershipTypeSelected && <OwnershipTypeSelector />}
            </AllowedTermsWrapper>
            <CardinalityField paramsField="ownershipParams" />
        </>
    );
};

export default OwnershipQuestion;
