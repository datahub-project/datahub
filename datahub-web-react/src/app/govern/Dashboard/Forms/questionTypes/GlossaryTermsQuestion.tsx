import { Form, Tooltip } from 'antd';
import { CheckboxChangeEvent } from 'antd/lib/checkbox';
import React, { useState } from 'react';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { EntityType, GlossaryTerm } from '@src/types.generated';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { AllowedItemsWrapper, StyledCheckbox, StyledLabel, SelectorWrapper } from '../styledComponents';
import CardinalityField from './CardinalityField';
import GlossaryTermsSelector from './GlossaryTermsSelector';

export default function GlossaryTermsQuestion() {
    const entityRegistry = useEntityRegistryV2();
    const form = Form.useFormInstance();
    const allowedTerms =
        form.getFieldValue(['glossaryTermsParams', 'allowedTerms']) ||
        form.getFieldValue(['glossaryTermsParams', 'resolvedAllowedTerms']);
    const [anyTermsSelected, setAnyTermsSelected] = useState(allowedTerms ? !allowedTerms.length : true);
    const initialOptions =
        allowedTerms?.map((term: GlossaryTerm) => ({
            value: term.urn,
            label: entityRegistry.getDisplayName(term.type, term),
            id: term.urn,
            isParent: term.type === EntityType.GlossaryNode,
            parentId: term.parentNodes?.nodes?.[0]?.urn,
            entity: term,
        })) || [];
    function handleAllowedTermsChange(e: CheckboxChangeEvent) {
        const allowAnyTerm = !e.target.checked;
        setAnyTermsSelected(allowAnyTerm);
        if (allowAnyTerm) {
            form.setFieldValue(['glossaryTermsParams', 'allowedTerms'], undefined);
            form.setFieldValue(['glossaryTermsParams', 'resolvedAllowedTerms'], undefined);
        }
    }

    const onUpdate = (values: SelectOption[]) => {
        if (values.length) {
            const terms = values.map((v) => v.entity).filter((r) => !!r);
            form.setFieldValue(['glossaryTermsParams', 'allowedTerms'], terms);
        } else {
            form.setFieldValue(['glossaryTermsParams', 'allowedTerms'], undefined);
        }
    };

    return (
        <>
            <CardinalityField paramsField="glossaryTermsParams" inputType="glossary terms" />
            <AllowedItemsWrapper>
                <StyledCheckbox checked={!anyTermsSelected} onChange={handleAllowedTermsChange} />
                <Tooltip title="If left unchecked, then any glossary term will be allowed" showArrow={false}>
                    <StyledLabel>Restrict responses to specific glossary terms</StyledLabel>
                </Tooltip>
                {!anyTermsSelected && (
                    <SelectorWrapper>
                        <GlossaryTermsSelector
                            onUpdate={onUpdate}
                            initialOptions={initialOptions}
                            label="Allowed Glossary Terms:"
                        />
                    </SelectorWrapper>
                )}
            </AllowedItemsWrapper>
        </>
    );
}
