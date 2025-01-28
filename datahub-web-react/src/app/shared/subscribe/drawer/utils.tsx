import _ from 'lodash';

/**
 * Remove the GraphQL __typename fields from any object
 * @param obj
 * @returns {T}
 */
export function removeNestedTypeNames<T>(obj: T): T {
    // Check if the argument is an object and not null
    if (typeof obj !== 'object' || obj === null) return obj;
    // Shallow clone object so we're not modifying it directly
    // NOTE: the recursive call below will effectively do a deep clone where necessary
    const clonedObj = _.clone(obj as object);

    // Iterate over object properties
    Object.keys(clonedObj).forEach((prop) => {
        if (prop === '__typename') {
            // Remove __typename property
            delete clonedObj[prop];
        } else if (typeof clonedObj[prop] === 'object' && clonedObj[prop] !== null) {
            // Recurse for nested objects and arrays
            clonedObj[prop] = removeNestedTypeNames(clonedObj[prop]);
        }
    });
    return clonedObj as T;
}

export function cleanAssertionDescription(builderStateData) {
    const newBuilderStateData = { ...builderStateData };
    // Create a shallow copy of the assertion to avoid modifying the original object
    const assertion = { ...newBuilderStateData.assertion };
    // Description support not added in backend for testAssertion api, so deleting it here won't reflect in the original assertion
    delete assertion.description;
    newBuilderStateData.assertion = assertion;
    const { type } = assertion;
    return { newBuilderStateData, type };
}
