import type { FormRule } from 'antd';

import { FieldType, FieldsValues, RecipeField } from '@app/ingestV2/source/builder/RecipeForm/common';

const ALLOW_DENY_WIDGET = 'allow_deny';
const PASSWORD_WIDGET = 'password';

const WIDGET_TO_FIELD_TYPE: Record<string, FieldType> = {
    text: FieldType.TEXT,
    number: FieldType.TEXT,
    toggle: FieldType.BOOLEAN,
    select: FieldType.SELECT,
    list: FieldType.LIST,
    keyvalue: FieldType.DICT,
    textarea: FieldType.TEXTAREA,
};

const URI_FORMATS = new Set(['uri', 'url']);

export type SchemaFormFieldOption = {
    label: string;
    value: string;
};

export type SchemaFormFieldConstraints = {
    minimum?: number;
    maximum?: number;
    exclusive_minimum?: number;
    exclusive_maximum?: number;
    min_length?: number;
    max_length?: number;
    pattern?: string;
    format?: string;
};

export type SchemaFormField = {
    name: string;
    label: string;
    field_path: string;
    widget: string;
    icon?: string;
    description?: string;
    required?: boolean;
    secret?: boolean;
    deprecated?: boolean;
    default?: unknown;
    placeholder?: string;
    options?: SchemaFormFieldOption[];
    constraints?: SchemaFormFieldConstraints;
    always_show?: boolean;
    depends_on?: string;
    enabled_when?: unknown;
    group_fields?: SchemaFormField[];
};

function resolveFieldType(f: SchemaFormField): FieldType {
    if (f.widget === PASSWORD_WIDGET || f.secret) {
        return FieldType.SECRET;
    }
    return WIDGET_TO_FIELD_TYPE[f.widget] ?? FieldType.TEXT;
}

function buildExclusiveRule(exclusiveValue: number, isMinimum: boolean): FormRule {
    const message = isMinimum ? `Must be > ${exclusiveValue}` : `Must be < ${exclusiveValue}`;
    return {
        type: 'number',
        validator: (_, value) => {
            const isValid =
                value === null || value === undefined || (isMinimum ? value > exclusiveValue : value < exclusiveValue);
            return isValid ? Promise.resolve() : Promise.reject(new Error(message));
        },
    };
}

function buildRules(f: SchemaFormField): FormRule[] | null {
    const c = f.constraints;
    if (!c) {
        return null;
    }

    const rules: FormRule[] = [];

    if (c.exclusive_minimum !== undefined) {
        rules.push(buildExclusiveRule(c.exclusive_minimum, true));
    }
    if (c.exclusive_maximum !== undefined) {
        rules.push(buildExclusiveRule(c.exclusive_maximum, false));
    }
    if (c.minimum !== undefined || c.maximum !== undefined) {
        rules.push({
            type: 'number',
            ...(c.minimum !== undefined ? { min: c.minimum } : {}),
            ...(c.maximum !== undefined ? { max: c.maximum } : {}),
            message: `Must be between ${c.minimum ?? '-∞'} and ${c.maximum ?? '∞'}`,
        });
    }
    if (c.min_length !== undefined || c.max_length !== undefined) {
        rules.push({
            ...(c.min_length !== undefined ? { min: c.min_length } : {}),
            ...(c.max_length !== undefined ? { max: c.max_length } : {}),
            message: `Must be between ${c.min_length ?? 0} and ${c.max_length ?? '∞'} characters`,
        });
    }
    if (c.pattern !== undefined) {
        rules.push({ pattern: new RegExp(c.pattern), message: `Must match pattern ${c.pattern}` });
    }
    if (c.format === 'email') {
        rules.push({ type: 'email' });
    } else if (c.format !== undefined && URI_FORMATS.has(c.format)) {
        rules.push({ type: 'url' });
    }

    return rules.length ? rules : null;
}

function buildDynamicHidden(f: SchemaFormField): ((values: FieldsValues) => boolean) | undefined {
    if (!f.depends_on) {
        return undefined;
    }
    const dependsOn = f.depends_on;
    const enabledWhen = f.enabled_when;
    return (values: FieldsValues) => values?.[dependsOn] !== enabledWhen;
}

function buildAllowDenyFields(f: SchemaFormField): RecipeField[] {
    const shared = {
        tooltip: f.description ?? '',
        type: FieldType.LIST,
        required: !!f.required,
        placeholder: f.placeholder,
        options: f.options,
        rules: buildRules(f),
        dynamicHidden: buildDynamicHidden(f),
    };

    return [
        {
            ...shared,
            name: `${f.name}.allow`,
            label: `${f.label} — Allow`,
            fieldPath: `${f.field_path}.allow`,
        },
        {
            ...shared,
            name: `${f.name}.deny`,
            label: `${f.label} — Deny`,
            fieldPath: `${f.field_path}.deny`,
        },
    ];
}

function buildLeafField(f: SchemaFormField): RecipeField {
    return {
        name: f.name,
        label: f.label,
        tooltip: f.description ?? '',
        type: resolveFieldType(f),
        fieldPath: f.field_path,
        rules: buildRules(f),
        required: !!f.required,
        placeholder: f.placeholder,
        options: f.options,
        dynamicHidden: buildDynamicHidden(f),
    };
}

export function adaptField(f: SchemaFormField): RecipeField[] {
    if (f.widget === ALLOW_DENY_WIDGET) {
        return buildAllowDenyFields(f);
    }
    return [buildLeafField(f)];
}
