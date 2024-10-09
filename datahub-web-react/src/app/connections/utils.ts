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

// Function to recursively merge form values into the existing config structure
export const mergeConfig = (config, formValues) => {
    const nestedFormValues = transformDotNotationToNested(formValues);
    return Object.keys(config).reduce((acc, key) => {
        if (typeof config[key] === 'object' && !Array.isArray(config[key]) && config[key] !== null) {
            // If it's a nested object, recursively merge
            acc[key] = mergeConfig(config[key], nestedFormValues[key] || {});
        } else {
            // If form value exists for this key, use it; otherwise, keep the existing config value
            acc[key] = nestedFormValues[key] !== undefined ? nestedFormValues[key] : config[key];
        }
        return acc;
    }, {});
};
