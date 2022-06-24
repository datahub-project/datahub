export const validRecipe = {
    source: {
        type: 'postgres',
        config: {
            host_port: 'localhost:5432',
        },
    },
    sink: {
        type: 'datahub-rest',
        config: {
            server: 'http://localhost:3000/api/gms',
            token: 'TEST_TOKEN',
        },
    },
};

export const validRecipeNoSink = {
    source: { ...validRecipe.source },
};

export const invalidTypeRecipe = {
    ...validRecipe,
    source: {
        ...validRecipe.source,
        type: 'not_supported',
    },
};

export const missingSourceRecipe = {
    sink: { ...validRecipe.sink },
};

export const invalidNestedFieldRecipe = {
    ...validRecipe,
    source: {
        ...validRecipe.source,
        config: {
            host_port: 1234,
        },
    },
};
