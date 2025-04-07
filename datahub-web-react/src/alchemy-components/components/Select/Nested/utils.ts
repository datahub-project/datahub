import { SelectOption } from './types';

export function getChainOfParents(option: SelectOption | undefined, options: SelectOption[]) {
    if (option === undefined) return [];
    if (option.parentValue === undefined) return [option];

    const parentOption = options.find((parentOptionCandidate) => parentOptionCandidate.value === option.parentValue);
    return [option, ...getChainOfParents(parentOption, options)];
}

export function filterNestedSelectOptions(
    options: SelectOption[],
    query: string,
) {
    if (query === '') return options;

    const filteredOptionsWithoutParents = options.filter((option) => option.label.toLowerCase().includes(query.toLowerCase()));
    const valuesOfFilteredOptions = filteredOptionsWithoutParents.map((option) => option.value);

    const parentOptions = Array.from(
        new Set(filteredOptionsWithoutParents.map((option) => getChainOfParents(option, options)).flat()),
    );
    const valuesOfParentOptions = parentOptions.map((option) => option.value);
    const valuesToKeep = new Set([...valuesOfFilteredOptions, ...valuesOfParentOptions]);

    const filteredOptionsWithParents = options.filter((option) => valuesToKeep.has(option.value));

    return filteredOptionsWithParents.map((option) => {
        if (!option.isParent) return option;

        const remainsToBeParent = filteredOptionsWithParents.find(
            (childOption) => childOption.parentValue === option.value,
        );

        if (remainsToBeParent) return option;

        return {
            ...option,
            isParent: false,
        };
    });
}
