import { TestAction, TestActions } from '@src/app/automations/types';
import { SelectPredicate, TestDefinition, TestPredicate } from '../../../types';
import { DEFAULT_TEST_DEFINITION } from '../../types';
import { AND, CONDITIONS, MATCH, NOT, ON, OR, RULES, TYPES } from './constants';
import { ACTION_TYPES } from './builder/property/types/action';
import { ValueTypeId } from './builder/property/types/values';

/**
 * Recursively transforms a raw predicate block into the latest
 * compatible TestPredicate format.
 *
 * @param rawPredicate the predicate object received from parsing a raw test json.
 */
const transformPredicate = (rawPredicate: any): TestPredicate => {
    if (!rawPredicate) {
        return [];
    }
    if (Array.isArray(rawPredicate)) {
        const finalPredicates: TestPredicate[] = [];
        rawPredicate
            .filter((pred) => pred)
            .forEach((pred) => {
                finalPredicates.push(transformPredicate(pred) as TestPredicate);
            });
        return finalPredicates;
    }
    if (AND in rawPredicate) {
        const andPredicate = rawPredicate;
        return {
            and: transformPredicate(andPredicate.and),
        };
    }
    if (OR in rawPredicate) {
        const orPredicate = rawPredicate;
        return {
            or: transformPredicate(orPredicate.or),
        };
    }
    if (NOT in rawPredicate) {
        const notPredicate = rawPredicate;
        return {
            not: transformPredicate(notPredicate.not),
        };
    }
    return {
        property: rawPredicate.query || rawPredicate.property,
        operator: rawPredicate.operation || rawPredicate.operator,
        values:
            rawPredicate.params?.values ||
            (rawPredicate.params?.value && [rawPredicate.params?.value]) ||
            rawPredicate.values,
    };
};

/**
 *
 * Converts the 'on' block in a Metadata Test into the latest format
 * compatible with the UI test builder.
 *
 * @param rawTest the 'on' block as a result of parsing a JSON test definition
 */
const transformOn = (rawTest: any): SelectPredicate => {
    const finalOn = {
        types: rawTest.types,
        conditions: rawTest.conditions,
    };
    if (!(TYPES in rawTest)) {
        throw new Error('Test is missing required types selection');
    }
    if (MATCH in rawTest) {
        // Transform the "on" block to use latest semantics.
        finalOn.conditions = transformPredicate(rawTest.match);
    }
    if (CONDITIONS in rawTest) {
        // Transform the "conditions" block to use latest semantics.
        finalOn.conditions = transformPredicate(rawTest.conditions);
    }
    return finalOn as SelectPredicate;
};

/**
 * Converts a deserialized JSON test into the in-memory TestDefinition format.
 *
 * This includes mapping legacy field names into new field
 * names that are compatible with the React logic builder.
 *
 * @param rawTest the result of parsing a JSON test definition
 */
const transformTestDefinition = (rawTest: any): TestDefinition => {
    const finalObject = rawTest;
    if (ON in rawTest) {
        // Transform the "on" block to use latest semantics.
        finalObject.on = transformOn(rawTest.on);
    } else {
        throw new Error('Test is missing required selection block');
    }
    if (RULES in rawTest) {
        // Transform the "rules" block to use latest semantics.
        finalObject.rules = transformPredicate(rawTest.rules);
    } else {
        throw new Error('Test is missing required rules block');
    }
    return finalObject as TestDefinition;
};

/**
 * Converts a json-serialized test definition into a TestDefinition
 * type that is easier to work with in memory.
 *
 * @param json the test definition serialized as JSON,
 * or undefined if the input is undefined.
 */
export const deserializeTestDefinition = (json: string): TestDefinition => {
    try {
        const object = JSON.parse(json);
        return transformTestDefinition(object);
    } catch (e) {
        console.warn(`Caught exception while attempting to transform existing test JSON`, e);
        return DEFAULT_TEST_DEFINITION;
    }
};

/**
 * Converts a TestDefinition javascript type back into a valid json-serialized
 * test definition.
 *
 * @param param0 a test definition object
 */
export const serializeTestDefinition = ({ on, rules, actions }: TestDefinition) => {
    return JSON.stringify({
        on: (on && on.types && on) || (on && { ...on, types: [] }) || { types: [] },
        rules: (rules && rules) || [],
        actions,
    });
};

export const validateActionList = (actionList?: TestAction[]): TestValidationResult | null => {
    if (!actionList) return null;

    const invalidAction = actionList.find((action) => {
        const actionValues = action?.values;
        const actionTypeDef = ACTION_TYPES.find((type) => type.id === action?.type);

        return actionTypeDef?.valueType === ValueTypeId.URN_LIST && (!actionValues || actionValues.length < 1);
    });

    if (invalidAction) {
        const actionTypeDef = ACTION_TYPES.find((type) => type.id === invalidAction?.type);
        return {
            isValid: false,
            message: `Values cannot be empty for action type: ${actionTypeDef?.displayName}`,
        };
    }

    return null;
};

export const validateActions = (actions?: TestActions): TestValidationResult => {
    const passingValidation = validateActionList(actions?.passing);
    if (passingValidation) return passingValidation;

    const failingValidation = validateActionList(actions?.failing);
    if (failingValidation) return failingValidation;

    return {
        isValid: true,
    };
};

/**
 * Result returned after doing a shallow validation of the
 * test JSON.
 */
export type TestValidationResult = {
    isValid: boolean;
    message?: string;
};

/**
 * Validates that the JSON provided matches the expected structure, before
 * sending it to the server for use.
 *
 * Note that this method does not do DEEP validation of the JSON definition.
 * For example, we do not check validity of the nested test predicate structure.
 *
 * That will be validated on the server when the test is actually updated or validated.
 *
 * @param json the test definition, serialized as json
 */
export const validateJsonDefinition = (json: string): TestValidationResult => {
    try {
        const test: TestDefinition = JSON.parse(json);
        if (!(ON in test)) {
            return {
                isValid: false,
                message: "The test is missing selection criteria! ('on')",
            };
        }
        const { on } = test;
        if (!(TYPES in on) || !Array.isArray(on.types)) {
            return {
                isValid: false,
                message: 'The selection criteria is invalid. Invalid types.',
            };
        }
        if (!(RULES in test) || test.rules === null) {
            return {
                isValid: false,
                message: "The test is missing rules criteria! ('rules')",
            };
        }
        return validateActions(test.actions);
    } catch (e: unknown) {
        console.error(`Failed to parse JSON: ${e}`);
        return {
            isValid: false,
            message: 'Failed to parse test definition. Please check your YAML.',
        };
    }
};
