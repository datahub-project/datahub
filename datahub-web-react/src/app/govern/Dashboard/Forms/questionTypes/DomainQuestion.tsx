import { Form, Tooltip } from 'antd';
import { CheckboxChangeEvent } from 'antd/lib/checkbox';
import React, { useState } from 'react';
import { AllowedItemsWrapper, StyledCheckbox, StyledLabel } from '../styledComponents';
import DomainsSelector from './DomainsSelector';

export default function DomainsQuestion() {
    const form = Form.useFormInstance();
    const allowedDomains = form.getFieldValue(['domainParams', 'allowedDomains']);
    const [anyDomainsSelected, setAnyDomainSelected] = useState(allowedDomains ? !allowedDomains.length : true);

    function handleAllowedDomainsChange(e: CheckboxChangeEvent) {
        const allowAnyDomain = !e.target.checked;
        setAnyDomainSelected(allowAnyDomain);
        if (allowAnyDomain) {
            form.setFieldValue(['domainParams', 'allowedDomains'], undefined);
        }
    }

    return (
        <>
            <AllowedItemsWrapper>
                <StyledCheckbox checked={!anyDomainsSelected} onChange={handleAllowedDomainsChange} />
                <Tooltip title="If left unchecked, then any domain will be allowed" showArrow={false}>
                    <StyledLabel>Restrict responses to specific domains</StyledLabel>
                </Tooltip>
                {!anyDomainsSelected && <DomainsSelector />}
            </AllowedItemsWrapper>
        </>
    );
}
