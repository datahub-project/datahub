import { getEnvironmentLabel } from '@app/sharedV2/customFabricType/customFabricType.utils';

import { CustomFabricType, FabricType } from '@types';

export type EnvironmentOption = { value: string; label: string };

// OTHER stands for "a custom environment" in GraphQL output; it is never a stored value.
const BUILT_IN_ENVIRONMENTS = Object.values(FabricType).filter((env) => env !== FabricType.Other);

/**
 * Options for an ENV policy filter: the built-in environments, then the registered custom ones by
 * display name. A selected value that is neither (e.g. a deleted custom environment) stays listed so
 * it can be removed.
 */
export function getEnvironmentOptions(
    customEnvironments: CustomFabricType[],
    selectedValues: string[],
): EnvironmentOption[] {
    const options: EnvironmentOption[] = BUILT_IN_ENVIRONMENTS.map((env) => ({ value: env, label: env }));
    customEnvironments.forEach((environment) =>
        options.push({ value: environment.id, label: getEnvironmentLabel(environment.id, environment) }),
    );
    selectedValues
        .filter((value) => !options.some((option) => option.value === value))
        .forEach((value) => options.push({ value, label: value }));
    return options;
}
