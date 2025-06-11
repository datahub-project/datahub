// Function to convert dot-delimited keys to a nested structure
export const transformDotNotationToNested = (jsonString) => {
    const parsedData = typeof jsonString === 'string' ? JSON.parse(jsonString) : jsonString;
    const result = {};

    Object.keys(parsedData || jsonString).forEach((key) => {
        const value = parsedData[key];
        const keyParts = key.split('.');

        // Start building the nested structure
        keyParts.reduce((acc, part, index) => {
            if (index === keyParts.length - 1) {
                acc[part] = value;
            } else if (!acc[part]) {
                acc[part] = {};
            }
            return acc[part];
        }, result);
    });

    return result;
};

// Function to recursively merge config and form values, including all keys from either object
export const mergeConfig = (config, formValues) => {
    const nestedFormValues = transformDotNotationToNested(formValues);

    const allKeys = new Set([...Object.keys(config || {}), ...Object.keys(nestedFormValues || {})]);

    const result = {};
    allKeys.forEach((key) => {
        const configVal = config?.[key];
        const formVal = nestedFormValues?.[key];

        if (
            typeof configVal === 'object' &&
            !Array.isArray(configVal) &&
            configVal !== null &&
            typeof formVal === 'object' &&
            !Array.isArray(formVal) &&
            formVal !== null
        ) {
            result[key] = mergeConfig(configVal, formVal);
        } else {
            result[key] = formVal !== undefined ? formVal : configVal;
        }
    });

    return result;
};
