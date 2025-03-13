import { defaultFilteringPredicate } from '../utils';
import { FilteringPredicate, SelectOption } from './types';

export function getChainOfParents(option: SelectOption | undefined, options: SelectOption[]) {
    if (option === undefined) return [];
    if (option.parentValue === undefined) return [option];

    const parentOption = options.find((parentOptionCandidate) => parentOptionCandidate.value === option.parentValue);
    return [option, ...getChainOfParents(parentOption, options)];
}

export function filterNestedSelectOptions(
    options: SelectOption[],
    query: string,
    filteringPredicate?: FilteringPredicate,
) {
    if (query === '') return options;

    const finalFilteringPredicate = filteringPredicate ?? defaultFilteringPredicate;
    const filteredOptionsWithoutParents = options.filter((option) => finalFilteringPredicate(option, query));
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

export function areOptionsDifferent(optionsA: SelectOption[], optionsB: SelectOption[]) {
    const toJson = (options: SelectOption[]) =>
        JSON.stringify(
            options.map((option) => {
                // handle non-string labels as it is a ReactNode
                const label = typeof option.label === 'string' ? option.label : option.value;
                return { ...option, label };
            }),
        );

    const jsonOptionsA = toJson(optionsA);
    const jsonOptionsB = toJson(optionsB);

    return jsonOptionsA !== jsonOptionsB;
}
