import { Form, Radio, RadioChangeEvent, Space } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { FieldLabel, StyledRadioGroup } from '../styledComponents';
import DomainsSelector from './DomainsSelector';

const AllowedDomainsWrapper = styled.div`
    margin-bottom: 24px;
`;

export default function DomainsQuestion() {
    const form = Form.useFormInstance();
    const allowedDomains = form.getFieldValue(['domainParams', 'allowedDomains']);
    const [anyDomainsSelected, setAnyDomainSelected] = useState(allowedDomains ? !allowedDomains.length : true);

    function handleAllowedDomainsChange(e: RadioChangeEvent) {
        const allowAnyDomain = e.target.value;
        setAnyDomainSelected(allowAnyDomain);
        if (allowAnyDomain) {
            form.setFieldValue(['domainParams', 'allowedDomains'], undefined);
        }
    }

    return (
        <>
            <AllowedDomainsWrapper>
                <FieldLabel>Allowed Domains</FieldLabel>
                <StyledRadioGroup value={anyDomainsSelected} onChange={handleAllowedDomainsChange}>
                    <Space direction="vertical">
                        <Radio value>Any Domains</Radio>
                        <Radio value={false}>Domains in a specific set</Radio>
                    </Space>
                </StyledRadioGroup>
                {!anyDomainsSelected && <DomainsSelector />}
            </AllowedDomainsWrapper>
        </>
    );
}
