import { getEntityDisplayType } from '@app/searchV2/autoCompleteV2/utils';
import EntityRegistry from '@src/app/entityV2/EntityRegistry';
import { capitalizeFirstLetterOnly } from '@src/app/shared/textUtil';
import { EntityType } from '@src/types.generated';

// Mock the dependencies
vi.mock('@src/app/shared/textUtil', () => ({
    capitalizeFirstLetterOnly: vi.fn(),
}));

// Mock Entity type
interface MockEntity {
    type: EntityType;
    urn: string;
    [key: string]: any;
}

describe('getEntityDisplayType', () => {
    let mockRegistry: EntityRegistry;
    let mockCapitalizeFirstLetterOnly: any;

    beforeEach(() => {
        // Reset all mocks
        vi.clearAllMocks();

        // Create a mock registry
        mockRegistry = {
            getGenericEntityProperties: vi.fn(),
            getFirstSubType: vi.fn(),
            getEntityName: vi.fn(),
        } as any;

        // Setup the mock function
        mockCapitalizeFirstLetterOnly = vi.mocked(capitalizeFirstLetterOnly);
    });

    describe('when entity has a subtype', () => {
        it('should return capitalized subtype when subtype exists', () => {
            const mockEntity: MockEntity = {
                type: EntityType.Dataset,
                urn: 'urn:li:dataset:test',
            };

            const mockProperties = {
                subTypes: {
                    typeNames: ['view', 'table'],
                },
            };

            // Setup mock responses
            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue('view');
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Dataset');
            mockCapitalizeFirstLetterOnly.mockReturnValue('View');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            expect(mockRegistry.getGenericEntityProperties).toHaveBeenCalledWith(EntityType.Dataset, mockEntity);
            expect(mockRegistry.getFirstSubType).toHaveBeenCalledWith(mockProperties);
            expect(capitalizeFirstLetterOnly).toHaveBeenCalledWith('view');
            expect(mockRegistry.getEntityName).toHaveBeenCalledWith(EntityType.Dataset);
            expect(result).toBe('View');
        });

        it('should handle complex subtype names correctly', () => {
            const mockEntity: MockEntity = {
                type: EntityType.Dashboard,
                urn: 'urn:li:dashboard:test',
            };

            const mockProperties = {
                subTypes: {
                    typeNames: ['operational_dashboard'],
                },
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue('operational_dashboard');
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Dashboard');
            mockCapitalizeFirstLetterOnly.mockReturnValue('Operational_dashboard');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            expect(capitalizeFirstLetterOnly).toHaveBeenCalledWith('operational_dashboard');
            expect(result).toBe('Operational_dashboard');
        });

        it('should handle empty string subtype', () => {
            const mockEntity: MockEntity = {
                type: EntityType.Container,
                urn: 'urn:li:container:test',
            };

            const mockProperties = {
                subTypes: {
                    typeNames: [''],
                },
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue('');
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Container');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            // Empty string is falsy, so ternary should return entityName directly
            expect(capitalizeFirstLetterOnly).not.toHaveBeenCalled();
            expect(result).toBe('Container');
        });
    });

    describe('when entity has no subtype', () => {
        it('should return entity name when subtype is undefined', () => {
            const mockEntity: MockEntity = {
                type: EntityType.DataJob,
                urn: 'urn:li:dataJob:test',
            };

            const mockProperties = {
                name: 'Test Data Job',
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue(undefined);
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Data Job');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            expect(mockRegistry.getFirstSubType).toHaveBeenCalledWith(mockProperties);
            expect(capitalizeFirstLetterOnly).not.toHaveBeenCalled();
            expect(mockRegistry.getEntityName).toHaveBeenCalledWith(EntityType.DataJob);
            expect(result).toBe('Data Job');
        });

        it('should return entity name when subtype is null', () => {
            const mockEntity: MockEntity = {
                type: EntityType.GlossaryTerm,
                urn: 'urn:li:glossaryTerm:test',
            };

            const mockProperties = {
                subTypes: null,
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue(null);
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Glossary Term');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            expect(result).toBe('Glossary Term');
        });

        it('should return entity name when typeNames array is empty', () => {
            const mockEntity: MockEntity = {
                type: EntityType.Domain,
                urn: 'urn:li:domain:test',
            };

            const mockProperties = {
                subTypes: {
                    typeNames: [],
                },
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue(undefined);
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Domain');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            expect(result).toBe('Domain');
        });
    });

    describe('edge cases', () => {
        it('should handle null properties from registry', () => {
            const mockEntity: MockEntity = {
                type: EntityType.Chart,
                urn: 'urn:li:chart:test',
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(null);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue(undefined);
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Chart');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            expect(mockRegistry.getFirstSubType).toHaveBeenCalledWith(null);
            expect(result).toBe('Chart');
        });

        it('should handle undefined properties from registry', () => {
            const mockEntity: MockEntity = {
                type: EntityType.CorpUser,
                urn: 'urn:li:corpuser:test',
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(undefined);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue(undefined);
            mockRegistry.getEntityName = vi.fn().mockReturnValue('User');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            expect(mockRegistry.getFirstSubType).toHaveBeenCalledWith(undefined);
            expect(result).toBe('User');
        });

        it('should handle entity with unknown type', () => {
            const mockEntity: MockEntity = {
                type: 'UNKNOWN_TYPE' as EntityType,
                urn: 'urn:li:unknown:test',
            };

            const mockProperties = {};

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue(undefined);
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Unknown');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            expect(result).toBe('Unknown');
        });

        it('should handle capitalizeFirstLetterOnly returning undefined', () => {
            const mockEntity: MockEntity = {
                type: EntityType.Dataset,
                urn: 'urn:li:dataset:test',
            };

            const mockProperties = {
                subTypes: {
                    typeNames: ['view'],
                },
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue('view');
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Dataset');
            mockCapitalizeFirstLetterOnly.mockReturnValue(undefined);

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            // Function returns the result of capitalizeFirstLetterOnly directly when subtype exists
            expect(result).toBe(undefined);
        });

        it('should handle capitalizeFirstLetterOnly returning null', () => {
            const mockEntity: MockEntity = {
                type: EntityType.Dataset,
                urn: 'urn:li:dataset:test',
            };

            const mockProperties = {
                subTypes: {
                    typeNames: ['view'],
                },
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue('view');
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Dataset');
            mockCapitalizeFirstLetterOnly.mockReturnValue(null);

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            // Function returns the result of capitalizeFirstLetterOnly directly when subtype exists
            expect(result).toBe(null);
        });
    });

    describe('registry method calls', () => {
        it('should call registry methods in correct order', () => {
            const mockEntity: MockEntity = {
                type: EntityType.Mlmodel,
                urn: 'urn:li:mlModel:test',
            };

            const mockProperties = {
                subTypes: {
                    typeNames: ['classification'],
                },
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue('classification');
            mockRegistry.getEntityName = vi.fn().mockReturnValue('ML Model');
            mockCapitalizeFirstLetterOnly.mockReturnValue('Classification');

            getEntityDisplayType(mockEntity, mockRegistry);

            // Verify the order of calls
            expect(mockRegistry.getGenericEntityProperties).toHaveBeenCalledBefore(mockRegistry.getFirstSubType as any);
            expect(mockRegistry.getFirstSubType).toHaveBeenCalledBefore(mockRegistry.getEntityName as any);
        });

        it('should pass correct arguments to registry methods', () => {
            const mockEntity: MockEntity = {
                type: EntityType.DataFlow,
                urn: 'urn:li:dataFlow:test',
                additionalProperty: 'test-value',
            };

            const mockProperties = { name: 'Test Flow' };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue(undefined);
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Data Flow');

            getEntityDisplayType(mockEntity, mockRegistry);

            expect(mockRegistry.getGenericEntityProperties).toHaveBeenCalledWith(EntityType.DataFlow, mockEntity);
            expect(mockRegistry.getFirstSubType).toHaveBeenCalledWith(mockProperties);
            expect(mockRegistry.getEntityName).toHaveBeenCalledWith(EntityType.DataFlow);
        });
    });

    describe('real-world scenarios', () => {
        it('should handle dataset with table subtype', () => {
            const mockEntity: MockEntity = {
                type: EntityType.Dataset,
                urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,default.users,PROD)',
            };

            const mockProperties = {
                subTypes: {
                    typeNames: ['table'],
                },
                platform: {
                    name: 'hive',
                },
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue('table');
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Dataset');
            mockCapitalizeFirstLetterOnly.mockReturnValue('Table');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            expect(result).toBe('Table');
        });

        it('should handle dashboard without subtype', () => {
            const mockEntity: MockEntity = {
                type: EntityType.Dashboard,
                urn: 'urn:li:dashboard:(looker,dashboard.1)',
            };

            const mockProperties = {
                platform: {
                    name: 'looker',
                },
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue(undefined);
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Dashboard');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            expect(result).toBe('Dashboard');
        });

        it('should handle ML model with custom subtype', () => {
            const mockEntity: MockEntity = {
                type: EntityType.Mlmodel,
                urn: 'urn:li:mlModel:(urn:li:dataPlatform:sagemaker,my-model,PROD)',
            };

            const mockProperties = {
                subTypes: {
                    typeNames: ['tensorflow_serving'],
                },
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue('tensorflow_serving');
            mockRegistry.getEntityName = vi.fn().mockReturnValue('ML Model');
            mockCapitalizeFirstLetterOnly.mockReturnValue('Tensorflow_serving');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            expect(result).toBe('Tensorflow_serving');
        });

        it('should handle container with multiple subtypes (uses first one)', () => {
            const mockEntity: MockEntity = {
                type: EntityType.Container,
                urn: 'urn:li:container:12345',
            };

            const mockProperties = {
                subTypes: {
                    typeNames: ['database', 'schema', 'table'],
                },
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue('database'); // Only returns first one
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Container');
            mockCapitalizeFirstLetterOnly.mockReturnValue('Database');

            const result = getEntityDisplayType(mockEntity, mockRegistry);

            expect(mockRegistry.getFirstSubType).toHaveBeenCalledWith(mockProperties);
            expect(capitalizeFirstLetterOnly).toHaveBeenCalledWith('database');
            expect(result).toBe('Database');
        });
    });

    describe('function behavior consistency', () => {
        it('should return appropriate types based on conditions', () => {
            const testCases = [
                {
                    entity: { type: EntityType.Dataset, urn: 'test1' },
                    subtype: 'view',
                    entityName: 'Dataset',
                    capitalizedResult: 'View',
                    expectedResult: 'View',
                },
                {
                    entity: { type: EntityType.Chart, urn: 'test2' },
                    subtype: undefined,
                    entityName: 'Chart',
                    capitalizedResult: undefined,
                    expectedResult: 'Chart',
                },
                {
                    entity: { type: EntityType.CorpUser, urn: 'test3' },
                    subtype: null,
                    entityName: 'User',
                    capitalizedResult: null,
                    expectedResult: 'User',
                },
                {
                    entity: { type: EntityType.Dataset, urn: 'test4' },
                    subtype: 'view',
                    entityName: 'Dataset',
                    capitalizedResult: undefined, // capitalizeFirstLetterOnly returns undefined
                    expectedResult: undefined,
                },
            ];

            testCases.forEach(({ entity, subtype, entityName, capitalizedResult, expectedResult }) => {
                mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue({});
                mockRegistry.getFirstSubType = vi.fn().mockReturnValue(subtype);
                mockRegistry.getEntityName = vi.fn().mockReturnValue(entityName);
                mockCapitalizeFirstLetterOnly.mockReturnValue(capitalizedResult);

                const result = getEntityDisplayType(entity as MockEntity, mockRegistry);

                expect(result).toBe(expectedResult);
            });
        });

        it('should be deterministic with same inputs', () => {
            const mockEntity: MockEntity = {
                type: EntityType.GlossaryTerm,
                urn: 'urn:li:glossaryTerm:test',
            };

            const mockProperties = {
                subTypes: {
                    typeNames: ['concept'],
                },
            };

            mockRegistry.getGenericEntityProperties = vi.fn().mockReturnValue(mockProperties);
            mockRegistry.getFirstSubType = vi.fn().mockReturnValue('concept');
            mockRegistry.getEntityName = vi.fn().mockReturnValue('Glossary Term');
            mockCapitalizeFirstLetterOnly.mockReturnValue('Concept');

            const result1 = getEntityDisplayType(mockEntity, mockRegistry);
            const result2 = getEntityDisplayType(mockEntity, mockRegistry);

            expect(result1).toBe(result2);
            expect(result1).toBe('Concept');
        });
    });
});
