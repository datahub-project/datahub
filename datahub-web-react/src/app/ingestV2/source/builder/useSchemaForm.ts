import { useMemo } from 'react';

import { SchemaFormField } from '@app/ingestV2/source/builder/SchemaForm/adaptField';
import bundle from '@app/ingestV2/source/builder/ui_forms.json';

export type SchemaFormSection = {
    key: string;
    title: string;
    icon?: string;
    expanded: boolean;
    fields: SchemaFormField[];
};

export type SchemaConnectorForm = {
    connector: string;
    display_name: string;
    total_properties: number;
    sections: SchemaFormSection[];
};

export const SCHEMA_FORMS_BY_CONNECTOR: Record<string, SchemaConnectorForm> = Object.fromEntries(
    (bundle as { forms: SchemaConnectorForm[] }).forms.map((f) => [f.connector, f]),
);

export function useSchemaForm(type: string): SchemaConnectorForm | undefined {
    return useMemo(() => SCHEMA_FORMS_BY_CONNECTOR[type], [type]);
}
