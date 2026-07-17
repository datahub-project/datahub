import { getAssetDescriptionDetails } from '@app/entityV2/shared/tabs/Documentation/utils';

import { EntityType } from '@types';

const mockActor = {
    urn: 'urn:li:corpuser:test',
    type: EntityType.CorpUser,
};

const propagationDocs = {
    documentation: {
        documentations: [
            {
                documentation: 'Doc 1',
                attribution: {
                    actor: mockActor,
                    time: 100,
                    sourceDetail: [{ key: 'inferred', value: 'true' }],
                },
            },
            {
                documentation: 'propagated',
                attribution: {
                    actor: mockActor,
                    time: 200,
                    sourceDetail: [{ key: 'propagated', value: 'true' }],
                },
            },
        ],
    },
    editableProperties: {
        description: undefined,
    },
    properties: {
        description: undefined,
    },
};

const uiAuthoredDocs = {
    documentation: {
        documentations: [
            {
                documentation: 'UI authored description',
                attribution: {
                    actor: mockActor,
                    time: 300,
                    sourceDetail: [{ key: 'ui', value: 'true' }],
                },
            },
            {
                documentation: 'propagated',
                attribution: {
                    actor: mockActor,
                    time: 200,
                    sourceDetail: [{ key: 'propagated', value: 'true' }],
                },
            },
            {
                documentation: 'inferred',
                attribution: {
                    actor: mockActor,
                    time: 100,
                    sourceDetail: [{ key: 'inferred', value: 'true' }],
                },
            },
        ],
    },
    editableProperties: {
        description: undefined,
    },
    properties: {
        description: undefined,
    },
};

const inferredDocs = {
    documentation: {
        documentations: [
            {
                documentation: 'Doc 1',
                attribution: {
                    actor: mockActor,
                    time: 300, // Later, but inferred
                    sourceDetail: [{ key: 'inferred', value: 'true' }],
                },
            },
            {
                documentation: 'Doc 2',
                attribution: {
                    actor: mockActor,
                    time: 200,
                    sourceDetail: [{ key: 'inferred', value: 'false' }],
                },
            },
        ],
    },
    editableProperties: {
        description: undefined,
    },
    properties: {
        description: undefined,
    },
};

describe('getAssetDescriptionDetails', () => {
    describe('basic cases', () => {
        it('returns empty string when all fields are undefined', () => {
            const result = getAssetDescriptionDetails({});
            expect(result.displayedDescription).toBe('');
            expect(result.isUsingDocumentationAspect).toBe(false);
            expect(result.isInferred).toBe(false);
        });

        it('returns defaultDescription if nothing else is present', () => {
            const result = getAssetDescriptionDetails({ defaultDescription: 'Default Desc' });
            expect(result.displayedDescription).toBe('Default Desc');
        });

        it('returns empty string if defaultDescription is empty', () => {
            const result = getAssetDescriptionDetails({ defaultDescription: '' });
            expect(result.displayedDescription).toBe('');
        });
    });

    describe('displayedDescription precedence', () => {
        it('returns edited description if present', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(propagationDocs)),
                editableProperties: { description: 'Edited Desc' },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('Edited Desc');
            expect(result.isUsingDocumentationAspect).toBe(false);
        });

        it('returns original description if edited is undefined', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(propagationDocs)),
                editableProperties: { description: undefined },
                properties: { description: 'Original Desc' },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('Original Desc');
            expect(result.isUsingDocumentationAspect).toBe(false);
        });

        it('returns propagated documentation when edited and original are empty', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(propagationDocs)),
                editableProperties: { description: '' },
                properties: { description: '' },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('propagated');
            expect(result.isUsingDocumentationAspect).toBe(true);
        });

        it('returns propagated documentation over other documentation', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(propagationDocs)),
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('propagated');
            expect(result.isUsingDocumentationAspect).toBe(true);
        });

        it('returns documentation when edited and original are undefined', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(inferredDocs)),
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('Doc 2');
            expect(result.isUsingDocumentationAspect).toBe(true);
        });

        it('returns documentation when edited is undefined and original is empty', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(inferredDocs)),
                editableProperties: { description: undefined },
                properties: { description: '' },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('Doc 2');
            expect(result.isUsingDocumentationAspect).toBe(true);
        });

        it('returns empty string when edited description is empty', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(inferredDocs)),
                editableProperties: { description: '' },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('');
            expect(result.isUsingDocumentationAspect).toBe(true);
        });

        it('returns documentation when original description is empty and edited is undefined', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(inferredDocs)),
                editableProperties: { description: undefined },
                properties: { description: '' },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('Doc 2'); // Falls through to documentation
            expect(result.isUsingDocumentationAspect).toBe(true);
        });

        it('returns later inferred documentation when enabled', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(inferredDocs)),
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties, enableInferredDescriptions: true });
            expect(result.displayedDescription).toBe('Doc 1'); // Falls through to documentation
            expect(result.isUsingDocumentationAspect).toBe(true);
        });
    });

    describe('documentation aspect logic', () => {
        it('returns most recent non-inferred documentation', () => {
            const entityProperties = {
                documentation: {
                    documentations: [
                        {
                            documentation: 'Old Doc',
                            attribution: {
                                actor: mockActor,
                                time: 100,
                                sourceDetail: [{ key: 'inferred', value: 'false' }],
                            },
                        },
                        {
                            documentation: 'New Doc',
                            attribution: {
                                actor: mockActor,
                                time: 300,
                                sourceDetail: [{ key: 'inferred', value: 'false' }],
                            },
                        },
                    ],
                },
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('New Doc');
            expect(result.isUsingDocumentationAspect).toBe(true);
        });

        it('returns empty string if documentation is empty', () => {
            const entityProperties = {
                documentation: {
                    documentations: [
                        {
                            documentation: '',
                            attribution: {
                                actor: mockActor,
                                time: 100,
                                sourceDetail: [{ key: 'inferred', value: 'false' }],
                            },
                        },
                    ],
                },
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('');
            expect(result.isUsingDocumentationAspect).toBe(true);
        });
    });

    describe('inferred documentation', () => {
        it('filters out inferred documentation when enableInferredDescriptions is false', () => {
            const entityProperties = {
                documentation: {
                    documentations: [
                        {
                            documentation: 'Inferred Doc',
                            attribution: {
                                actor: mockActor,
                                time: 300,
                                sourceDetail: [{ key: 'inferred', value: 'true' }],
                            },
                        },
                        {
                            documentation: 'User Doc',
                            attribution: {
                                actor: mockActor,
                                time: 200,
                                sourceDetail: [{ key: 'inferred', value: 'false' }],
                            },
                        },
                    ],
                },
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties, enableInferredDescriptions: false });
            expect(result.displayedDescription).toBe('User Doc');
            expect(result.isInferred).toBe(false);
        });

        it('includes inferred documentation when enableInferredDescriptions is true', () => {
            const entityProperties = {
                documentation: {
                    documentations: [
                        {
                            documentation: 'Inferred Doc',
                            attribution: {
                                actor: mockActor,
                                time: 300,
                                sourceDetail: [{ key: 'inferred', value: 'true' }],
                            },
                        },
                    ],
                },
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties, enableInferredDescriptions: true });
            expect(result.displayedDescription).toBe('Inferred Doc');
            expect(result.isInferred).toBe(true);
        });

        it('returns most recent inferred documentation when multiple inferred docs exist', () => {
            const entityProperties = {
                documentation: {
                    documentations: [
                        {
                            documentation: 'Old Inferred',
                            attribution: {
                                actor: mockActor,
                                time: 100,
                                sourceDetail: [{ key: 'inferred', value: 'true' }],
                            },
                        },
                        {
                            documentation: 'New Inferred',
                            attribution: {
                                actor: mockActor,
                                time: 300,
                                sourceDetail: [{ key: 'inferred', value: 'true' }],
                            },
                        },
                    ],
                },
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties, enableInferredDescriptions: true });
            expect(result.displayedDescription).toBe('New Inferred');
            expect(result.isInferred).toBe(true);
        });
    });

    describe('edge cases', () => {
        it('handles null entityProperties', () => {
            const result = getAssetDescriptionDetails({ entityProperties: null });
            expect(result.displayedDescription).toBe('');
            expect(result.isUsingDocumentationAspect).toBe(false);
            expect(result.isInferred).toBe(false);
        });

        it('handles missing documentation field', () => {
            const entityProperties = {
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('');
            expect(result.isUsingDocumentationAspect).toBe(false);
        });

        it('handles empty documentations array', () => {
            const entityProperties = {
                documentation: {
                    documentations: [],
                },
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('');
            expect(result.isUsingDocumentationAspect).toBe(false);
        });

        it('handles missing attribution fields', () => {
            const entityProperties = {
                documentation: {
                    documentations: [
                        {
                            documentation: 'Doc without attribution',
                        },
                    ],
                },
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('Doc without attribution');
            expect(result.isUsingDocumentationAspect).toBe(true);
        });

        it('handles missing sourceDetail in attribution', () => {
            const entityProperties = {
                documentation: {
                    documentations: [
                        {
                            documentation: 'Doc without sourceDetail',
                            attribution: {
                                actor: mockActor,
                                time: 100,
                            },
                        },
                    ],
                },
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('Doc without sourceDetail');
            expect(result.isUsingDocumentationAspect).toBe(true);
            expect(result.isInferred).toBe(false);
        });
    });

    describe('return object properties', () => {
        it('returns correct sourceDetail', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(propagationDocs)),
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.sourceDetail).toEqual([{ key: 'propagated', value: 'true' }]);
        });

        it('returns inferredDescription when using inferred documentation', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(inferredDocs)),
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties, enableInferredDescriptions: true });
            expect(result.inferredDescription).toBe('Doc 1');
        });

        it('returns undefined inferredDescription when not using inferred documentation', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(inferredDocs)),
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.inferredDescription).toBeUndefined();
        });
    });

    describe('UI-authored documentation', () => {
        it('returns UI-authored documentation over propagated documentation', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(uiAuthoredDocs)),
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('UI authored description');
            expect(result.isUsingDocumentationAspect).toBe(true);
            expect(result.isUiAuthored).toBe(true);
            expect(result.isPropagated).toBe(false);
        });

        it('returns uiAuthoredDescription when UI-authored doc exists', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(uiAuthoredDocs)),
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.uiAuthoredDescription).toBe('UI authored description');
            expect(result.propagatedDescription).toBe('propagated');
        });

        it('returns propagated documentation when UI-authored doc is not present', () => {
            const entityProperties = {
                documentation: {
                    documentations: [
                        {
                            documentation: 'propagated',
                            attribution: {
                                actor: mockActor,
                                time: 200,
                                sourceDetail: [{ key: 'propagated', value: 'true' }],
                            },
                        },
                    ],
                },
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('propagated');
            expect(result.isUsingDocumentationAspect).toBe(true);
            expect(result.isPropagated).toBe(true);
            expect(result.isUiAuthored).toBe(false);
        });

        it('prioritizes editableProperties.description over UI-authored documentation', () => {
            const entityProperties = {
                ...JSON.parse(JSON.stringify(uiAuthoredDocs)),
                editableProperties: { description: 'Edited in UI via EditableProperties' },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('Edited in UI via EditableProperties');
            expect(result.isUsingDocumentationAspect).toBe(false);
        });

        it('returns most recent UI-authored doc when multiple exist', () => {
            const entityProperties = {
                documentation: {
                    documentations: [
                        {
                            documentation: 'Old UI doc',
                            attribution: {
                                actor: mockActor,
                                time: 100,
                                sourceDetail: [{ key: 'ui', value: 'true' }],
                            },
                        },
                        {
                            documentation: 'New UI doc',
                            attribution: {
                                actor: mockActor,
                                time: 300,
                                sourceDetail: [{ key: 'ui', value: 'true' }],
                            },
                        },
                    ],
                },
                editableProperties: { description: undefined },
                properties: { description: undefined },
            };
            const result = getAssetDescriptionDetails({ entityProperties });
            expect(result.displayedDescription).toBe('New UI doc');
            expect(result.isUiAuthored).toBe(true);
        });
    });
});
