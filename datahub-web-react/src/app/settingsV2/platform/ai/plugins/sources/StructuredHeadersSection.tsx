import React from 'react';
import styled from 'styled-components';

import {
    StructuredHeaderField,
    StructuredHeadersConfig,
} from '@app/settingsV2/platform/ai/plugins/sources/pluginSources.types';
import { Input, colors } from '@src/alchemy-components';

import { AiPluginAuthType } from '@types';

// ---------------------------------------------------------------------------
// Styled components
// ---------------------------------------------------------------------------

const Section = styled.div`
    margin-top: 16px;
    padding-top: 16px;
    border-top: 1px solid ${colors.gray[100]};
`;

const FormField = styled.div`
    margin-bottom: 16px;
`;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type Props = {
    config: StructuredHeadersConfig;
    currentAuthType: AiPluginAuthType;
    headerValues: Record<string, string>;
    onHeaderValueChange: (headerKey: string, value: string) => void;
};

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

/** Renders a single labelled input for a structured header field. */
function StructuredHeaderInput({
    field,
    value,
    onChange,
}: {
    field: StructuredHeaderField;
    value: string;
    onChange: (val: string) => void;
}) {
    return (
        <FormField>
            <Input
                label={field.label}
                placeholder={field.placeholder}
                value={value}
                setValue={onChange}
                isRequired={field.required}
                helperText={field.helperText}
            />
        </FormField>
    );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

/**
 * Renders structured header inputs for a plugin source.
 * Filters fields by the current auth type.
 */
export function StructuredHeadersSection({ config, currentAuthType, headerValues, onHeaderValueChange }: Props) {
    const visibleFields = config.fields.filter(
        (field) => !field.visibleForAuthTypes || field.visibleForAuthTypes.includes(currentAuthType),
    );

    if (visibleFields.length === 0) return null;

    return (
        <Section>
            {visibleFields.map((field) => (
                <StructuredHeaderInput
                    key={field.headerKey}
                    field={field}
                    value={headerValues[field.headerKey] || ''}
                    onChange={(val) => onHeaderValueChange(field.headerKey, val)}
                />
            ))}
        </Section>
    );
}
