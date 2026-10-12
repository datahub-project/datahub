import { formatNumberWithoutAbbreviation } from '@app/shared/formatNumber';
import { parseMaybeStringAsFloatOrDefault } from '@app/shared/numberUtil';

import { AssertionStdOperator, AssertionStdParameters, VolumeAssertionInfo, VolumeAssertionType } from '@types';

export const getIsRowCountChange = (type: VolumeAssertionType) => {
    return [VolumeAssertionType.RowCountChange, VolumeAssertionType.IncrementingSegmentRowCountChange].includes(type);
};

export type VolumeOperatorKeyPart = 'AtLeast' | 'AtMost' | 'Between' | 'EqualTo';

export const getVolumeOperatorKeyPart = (operator: AssertionStdOperator): VolumeOperatorKeyPart | null => {
    switch (operator) {
        case AssertionStdOperator.GreaterThanOrEqualTo:
            return 'AtLeast';
        case AssertionStdOperator.LessThanOrEqualTo:
            return 'AtMost';
        case AssertionStdOperator.Between:
            return 'Between';
        case AssertionStdOperator.EqualTo:
            return 'EqualTo';
        default:
            return null;
    }
};

// A single value ("5") or a range ("5" and "10") — the explicit `kind` tag lets callers switch on
// it directly instead of a structural `'min' in parameterDescription` check, and TypeScript narrows
// exhaustively (removing `kind` or adding a third variant without updating every switch is a type error).
export type ParameterDescription = { kind: 'single'; value: string } | { kind: 'range'; min: string; max: string };

export const getParameterDescription = (parameters: AssertionStdParameters): ParameterDescription | undefined => {
    if (parameters.value) {
        return {
            kind: 'single',
            value: formatNumberWithoutAbbreviation(
                parseMaybeStringAsFloatOrDefault(parameters.value.value, parameters.value.value),
            ),
        };
    }
    if (parameters.minValue && parameters.maxValue) {
        return {
            kind: 'range',
            min: formatNumberWithoutAbbreviation(
                parseMaybeStringAsFloatOrDefault(parameters.minValue.value, parameters.minValue.value),
            ),
            max: formatNumberWithoutAbbreviation(
                parseMaybeStringAsFloatOrDefault(parameters.maxValue.value, parameters.maxValue.value),
            ),
        };
    }
    console.error('Invalid assertion parameters provided', parameters);
    return undefined;
};

// Both VolumeAssertionDescription.tsx and assertion/profile/summary/utils.tsx need to turn a
// ParameterDescription into the interpolation object for their i18next template — centralized here
// so the discrimination logic (and its exhaustiveness check) lives in one place, not two.
export type ParameterInterpolation = { parameter: string } | { min: string; max: string };

export const getParameterInterpolation = (
    parameterDescription: ParameterDescription | undefined,
): ParameterInterpolation => {
    if (!parameterDescription) {
        return { parameter: '' };
    }
    switch (parameterDescription.kind) {
        case 'range':
            return { min: parameterDescription.min, max: parameterDescription.max };
        case 'single':
            return { parameter: parameterDescription.value };
        default: {
            // Unreachable per the exhaustiveness check below (a new `kind` variant left unhandled here
            // is a compile error) — but if malformed data ever slips past the type system at runtime,
            // fail soft with an empty parameter rather than crashing the assertion description UI.
            const exhaustiveCheck: never = parameterDescription;
            console.error('Unhandled ParameterDescription kind:', exhaustiveCheck);
            return { parameter: '' };
        }
    }
};

type VolumeTypeField =
    | 'rowCountTotal'
    | 'rowCountChange'
    | 'incrementingSegmentRowCountTotal'
    | 'incrementingSegmentRowCountChange';

const getPropertyFromVolumeType = (type: VolumeAssertionType) => {
    switch (type) {
        case VolumeAssertionType.RowCountTotal:
            return 'rowCountTotal' as VolumeTypeField;
        case VolumeAssertionType.RowCountChange:
            return 'rowCountChange' as VolumeTypeField;
        case VolumeAssertionType.IncrementingSegmentRowCountTotal:
            return 'incrementingSegmentRowCountTotal' as VolumeTypeField;
        case VolumeAssertionType.IncrementingSegmentRowCountChange:
            return 'incrementingSegmentRowCountChange' as VolumeTypeField;
        default:
            throw new Error(`Unknown volume assertion type: ${type}`);
    }
};

export const getVolumeTypeInfo = (volumeAssertion: VolumeAssertionInfo) => {
    const result = volumeAssertion[getPropertyFromVolumeType(volumeAssertion.type)];
    if (!result) {
        return undefined;
    }
    return result;
};
