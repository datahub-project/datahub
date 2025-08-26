import uniqid from 'uniqid';

// Predicates available for selection
export const predicateOptions = [
    {
        label: 'Equals',
        value: 'equals',
    },
    {
        label: 'Not Equals',
        value: 'notEquals',
    },
    {
        label: 'Contains',
        value: 'contains',
    },
    {
        label: 'Does Not Contain',
        value: 'doesNotContain',
    },
    {
        label: 'Starts with',
        value: 'startsWith',
    },
    {
        label: 'Matches Regex',
        value: 'matchesRegex',
    },
    {
        label: 'Exists',
        value: 'exists',
    },
];

export const operatorOptions = [
    {
        label: 'And',
        value: 'and',
    },
    {
        label: 'Or',
        value: 'or',
    },
    {
        label: 'Not',
        value: 'not',
    },
];

export const transformConditions = (conditions, selectedAssetTypes) => {
    const trnsfmed = conditions.map((conditionGroup) =>
        Object.keys(conditionGroup).map((key) => {
            const hasConditions = !!conditionGroup[key][0];
            if (!hasConditions) return {};

            const keyData = conditionGroup[key][0];

            return {
                id: uniqid(),
                type: 'single',
                assetTypes: selectedAssetTypes,
                props: {
                    conditions: [],
                    showAssetCount: true,
                    operator: 'and',
                    field: keyData.property,
                    predicate: predicateOptions.find((option) => option.value === keyData.operator)?.value,
                    value: keyData.values,
                },
            };
        }),
    );

    const flatData = trnsfmed.flat();

    return flatData.filter((condition) => condition.props.field !== undefined);
};
