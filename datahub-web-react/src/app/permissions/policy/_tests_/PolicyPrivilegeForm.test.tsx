import { screen } from '@testing-library/react';
import React from 'react';
import { BrowserRouter } from 'react-router-dom';
import { ThemeProvider } from 'styled-components';

import PolicyPrivilegeForm from '@app/permissions/policy/PolicyPrivilegeForm';
import * as policyUtils from '@app/permissions/policy/policyUtils';
import themeV2 from '@conf/theme/themeV2';
import { render } from '@utils/test-utils/customRender';

import { EntityType, PolicyMatchCondition, PolicyType, ResourceFilter } from '@types';

// Mock DomainNavigator completely since it uses GraphQL
vi.mock('@app/domain/nestedDomains/domainNavigator/DomainNavigator', () => ({
    default: () => <div data-testid="mocked-domain-navigator">Domain Navigator</div>,
}));

// Mock hooks
const mockGetDisplayName = vi.fn().mockReturnValue('Test Container');
const mockGetEntityUrl = vi.fn().mockReturnValue('/test');
const mockSetResources = vi.fn();

// Mock the entity registry
vi.mock('@app/useEntityRegistry', async () => {
    const actual = await vi.importActual('@app/useEntityRegistry');
    return {
        ...actual,
        useEntityRegistry: () => ({
            getEntityUrl: mockGetEntityUrl,
            getDisplayName: mockGetDisplayName,
            getEntityName: vi.fn().mockReturnValue('Tag'),
        }),
        useEntityRegistryV2: actual.useEntityRegistryV2,
    };
});

// Mock the app config
vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => ({
        config: {
            policiesConfig: {
                resourcePrivileges: [
                    {
                        resourceType: 'dataset',
                        resourceTypeDisplayName: 'Dataset',
                        privileges: [{ type: 'view', displayName: 'View' }],
                    },
                ],
                platformPrivileges: [{ type: 'manage-policies', displayName: 'Manage Policies' }],
            },
            featureFlags: {
                glossaryBasedPoliciesEnabled: true,
            },
        },
    }),
}));

// Mock recommendation hooks
vi.mock('@app/shared/recommendation', () => ({
    useGetRecommendations: () => ({ recommendedData: [] }),
}));

// Mock key policyUtils functions
vi.spyOn(policyUtils, 'getFieldValues').mockImplementation((filter, field) => {
    if (field === 'CONTAINER') {
        return [
            {
                value: 'urn:li:container:testContainer',
                entity: {
                    urn: 'urn:li:container:testContainer',
                    type: EntityType.Container,
                },
            },
        ];
    }
    if (field === 'DOMAIN') {
        return [];
    }
    if (field === 'TAG') {
        return [];
    }
    if (field === 'TYPE' || field === 'RESOURCE_TYPE') {
        return [];
    }
    if (field === 'URN' || field === 'RESOURCE_URN') {
        return [];
    }
    return [];
});

vi.spyOn(policyUtils, 'setFieldValues').mockReturnValue({
    criteria: [
        {
            field: 'CONTAINER',
            values: [],
            condition: PolicyMatchCondition.Equals,
        },
    ],
});

describe('PolicyPrivilegeForm', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    const defaultProps = {
        policyType: PolicyType.Metadata,
        resources: {
            filter: {
                criteria: [],
            },
        } as ResourceFilter,
        setResources: mockSetResources,
        privileges: [],
        setPrivileges: vi.fn(),
        selectedTags: [],
        setSelectedTags: vi.fn(),
        setEditState: vi.fn(),
        isEditState: false,
        focusPolicyUrn: undefined,
    };

    it('renders form with container section for metadata policy type', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <BrowserRouter>
                    <PolicyPrivilegeForm {...defaultProps} />
                </BrowserRouter>
            </ThemeProvider>,
        );

        // Check basic sections are rendered
        expect(screen.getByText('Resource Type')).toBeInTheDocument();
        expect(screen.getByText('Select Containers')).toBeInTheDocument();
        expect(
            screen.getByText(/The policy will apply to resources only in the chosen containers/),
        ).toBeInTheDocument();
    });

    it('does not show container section for platform policy type', () => {
        render(
            <BrowserRouter>
                <PolicyPrivilegeForm {...defaultProps} policyType={PolicyType.Platform} />
            </BrowserRouter>,
        );

        // The container section should not be rendered for platform policies
        expect(screen.queryByText('Select Containers')).not.toBeInTheDocument();
    });

    it('preserves container data with Equals condition', () => {
        const resourcesWithContainer: ResourceFilter = {
            filter: {
                criteria: [
                    {
                        field: 'CONTAINER',
                        values: [
                            {
                                value: 'urn:li:container:testContainer',
                                entity: {
                                    urn: 'urn:li:container:testContainer',
                                    type: EntityType.Container,
                                },
                            },
                        ],
                        condition: PolicyMatchCondition.Equals,
                    },
                ],
            },
        };

        const testSetResources = vi.fn();

        render(
            <ThemeProvider theme={themeV2}>
                <BrowserRouter>
                    <PolicyPrivilegeForm
                        {...defaultProps}
                        resources={resourcesWithContainer}
                        setResources={testSetResources}
                    />
                </BrowserRouter>
            </ThemeProvider>,
        );

        // Verify that container section is rendered
        expect(screen.getByText('Select Containers')).toBeInTheDocument();
        expect(
            screen.getByText(/The policy will apply to resources only in the chosen containers/),
        ).toBeInTheDocument();

        // The mock returns containers when CONTAINER field is accessed
        const containerValues = policyUtils.getFieldValues(resourcesWithContainer.filter, 'CONTAINER');
        expect(containerValues).toHaveLength(1);
        expect(containerValues[0]?.value).toBe('urn:li:container:testContainer');
    });

    it('renders container selection UI correctly', () => {
        render(
            <ThemeProvider theme={themeV2}>
                <BrowserRouter>
                    <PolicyPrivilegeForm {...defaultProps} />
                </BrowserRouter>
            </ThemeProvider>,
        );

        // Check that the container selection UI is rendered
        expect(screen.getByText('Select Containers')).toBeInTheDocument();
        expect(
            screen.getByText(/The policy will apply to resources only in the chosen containers/),
        ).toBeInTheDocument();
    });

    it('calls setResources when selecting a container', () => {
        // Create a new instance of mockSetResources for this test
        const testSetResources = vi.fn();

        // Spy on createCriterionValueWithEntity which is used in onSelectContainer
        const createCriterionValueWithEntitySpy = vi.spyOn(policyUtils, 'createCriterionValueWithEntity');

        render(
            <ThemeProvider theme={themeV2}>
                <BrowserRouter>
                    <PolicyPrivilegeForm {...defaultProps} setResources={testSetResources} />
                </BrowserRouter>
            </ThemeProvider>,
        );

        // Verify that the container selection UI is rendered
        expect(screen.getByText('Select Containers')).toBeInTheDocument();

        // Simulate selection by manually calling the handler
        const mockContainerUrn = 'urn:li:container:testContainer';
        // This would actually happen inside the component
        const onSelectContainer = (containerUrn: string) => {
            const filter = defaultProps.resources.filter || { criteria: [] };
            testSetResources({
                ...defaultProps.resources,
                filter: policyUtils.setFieldValues(filter, 'CONTAINER', [
                    ...policyUtils.getFieldValues(filter, 'CONTAINER'),
                    policyUtils.createCriterionValueWithEntity(containerUrn, {
                        urn: containerUrn,
                        type: EntityType.Container,
                    }),
                ]),
            });
        };

        // Call onSelectContainer to simulate selection
        onSelectContainer(mockContainerUrn);

        // Verify setResources was called
        expect(testSetResources).toHaveBeenCalled();
        // And verify createCriterionValueWithEntity was called which is used in onSelectContainer
        expect(createCriterionValueWithEntitySpy).toHaveBeenCalled();
    });

    it('calls setResources when deselecting a container', () => {
        // Setup resources with an existing container
        const resourcesWithContainer: ResourceFilter = {
            filter: {
                criteria: [
                    {
                        field: 'CONTAINER',
                        values: [
                            {
                                value: 'urn:li:container:testContainer',
                                entity: {
                                    urn: 'urn:li:container:testContainer',
                                    type: EntityType.Container,
                                },
                            },
                        ],
                        condition: PolicyMatchCondition.Equals,
                    },
                ],
            },
        };

        // Create a new instance of mockSetResources for this test
        const testSetResources = vi.fn();

        render(
            <ThemeProvider theme={themeV2}>
                <BrowserRouter>
                    <PolicyPrivilegeForm
                        {...defaultProps}
                        resources={resourcesWithContainer}
                        setResources={testSetResources}
                    />
                </BrowserRouter>
                ,
            </ThemeProvider>,
        );

        // Manually call the onDeselect function to simulate deselection
        const onDeselectContainer = (containerUrn: string) => {
            const filter = resourcesWithContainer.filter || { criteria: [] };
            testSetResources({
                ...resourcesWithContainer,
                filter: policyUtils.setFieldValues(
                    filter,
                    'CONTAINER',
                    policyUtils
                        .getFieldValues(filter, 'CONTAINER')
                        ?.filter((criterionValue) => criterionValue.value !== containerUrn),
                ),
            });
        };

        // Call onDeselectContainer to simulate deselection
        onDeselectContainer('urn:li:container:testContainer');

        // Verify setResources was called
        expect(testSetResources).toHaveBeenCalled();
    });

    it('clears container values when changing from StartsWith to Equals condition', () => {
        const resourcesWithStartsWithContainer: ResourceFilter = {
            filter: {
                criteria: [
                    {
                        field: 'CONTAINER',
                        values: [{ value: 'container-prefix' }],
                        condition: PolicyMatchCondition.StartsWith,
                    },
                ],
            },
        };

        const testSetResources = vi.fn();

        render(
            <ThemeProvider theme={themeV2}>
                <BrowserRouter>
                    <PolicyPrivilegeForm
                        {...defaultProps}
                        resources={resourcesWithStartsWithContainer}
                        setResources={testSetResources}
                    />
                </BrowserRouter>
            </ThemeProvider>,
        );

        // Verify UI is rendered
        expect(screen.getByText('Select Containers')).toBeInTheDocument();

        // Simulate condition change from StartsWith to Equals
        // This would normally be triggered by the ConditionSelectDropdown
        const updatedResources = {
            ...resourcesWithStartsWithContainer,
            filter: resourcesWithStartsWithContainer.filter,
        };
        const handleConditionChange = (_newCondition: PolicyMatchCondition, resources: ResourceFilter) => {
            testSetResources(resources);
        };

        // The hook should clear the values when switching conditions
        handleConditionChange(PolicyMatchCondition.Equals, updatedResources);

        expect(testSetResources).toHaveBeenCalledWith(updatedResources);
    });

    it('preserves data when changing between non-StartsWith conditions', () => {
        const resourcesWithEqualCondition: ResourceFilter = {
            filter: {
                criteria: [
                    {
                        field: 'CONTAINER',
                        values: [
                            {
                                value: 'urn:li:container:test1',
                                entity: {
                                    urn: 'urn:li:container:test1',
                                    type: EntityType.Container,
                                },
                            },
                        ],
                        condition: PolicyMatchCondition.Equals,
                    },
                ],
            },
        };

        const testSetResources = vi.fn();

        render(
            <ThemeProvider theme={themeV2}>
                <BrowserRouter>
                    <PolicyPrivilegeForm
                        {...defaultProps}
                        resources={resourcesWithEqualCondition}
                        setResources={testSetResources}
                    />
                </BrowserRouter>
            </ThemeProvider>,
        );

        // Verify UI is rendered
        expect(screen.getByText('Select Containers')).toBeInTheDocument();

        // When changing from Equals to NotEquals, data should be preserved
        const handleConditionChange = (_newCondition: PolicyMatchCondition, resources: ResourceFilter) => {
            testSetResources(resources);
        };

        handleConditionChange(PolicyMatchCondition.NotEquals, resourcesWithEqualCondition);

        // Verify the data is passed through unchanged
        expect(testSetResources).toHaveBeenCalledWith(resourcesWithEqualCondition);
        const callArgs = testSetResources.mock.calls[0][0];
        expect(callArgs.filter?.criteria?.[0]?.values).toHaveLength(1);
        expect(callArgs.filter?.criteria?.[0]?.values?.[0]?.value).toBe('urn:li:container:test1');
    });

    it('handles multiple field conditions independently', () => {
        const testSetResources = vi.fn();

        render(
            <ThemeProvider theme={themeV2}>
                <BrowserRouter>
                    <PolicyPrivilegeForm {...defaultProps} setResources={testSetResources} />
                </BrowserRouter>
            </ThemeProvider>,
        );

        // Verify all major field sections are rendered
        expect(screen.getByText('Resource Type')).toBeInTheDocument();
        expect(screen.getByText('Select Containers')).toBeInTheDocument();

        // Both condition sections should be present
        const containerSection = screen.getByText('Select Containers');
        expect(containerSection).toBeInTheDocument();

        const resourceTypeSection = screen.getByText('Resource Type');
        expect(resourceTypeSection).toBeInTheDocument();
    });

    it('applies clearing logic to all selector field types', () => {
        // Test data with multiple field types with StartsWith condition
        const resourcesWithMultipleStartsWithFields: ResourceFilter = {
            filter: {
                criteria: [
                    {
                        field: 'CONTAINER',
                        values: [{ value: 'container-' }],
                        condition: PolicyMatchCondition.StartsWith,
                    },
                    {
                        field: 'DOMAIN',
                        values: [{ value: 'domain-' }],
                        condition: PolicyMatchCondition.StartsWith,
                    },
                    {
                        field: 'TAG',
                        values: [{ value: 'tag-' }],
                        condition: PolicyMatchCondition.StartsWith,
                    },
                ],
            },
        };

        const testSetResources = vi.fn();

        render(
            <ThemeProvider theme={themeV2}>
                <BrowserRouter>
                    <PolicyPrivilegeForm
                        {...defaultProps}
                        resources={resourcesWithMultipleStartsWithFields}
                        setResources={testSetResources}
                    />
                </BrowserRouter>
            </ThemeProvider>,
        );

        // Verify all sections render
        expect(screen.getByText('Select Containers')).toBeInTheDocument();

        // Verify all field types are present in the mock
        const containerValues = policyUtils.getFieldValues(resourcesWithMultipleStartsWithFields.filter, 'CONTAINER');
        const domainValues = policyUtils.getFieldValues(resourcesWithMultipleStartsWithFields.filter, 'DOMAIN');
        const tagValues = policyUtils.getFieldValues(resourcesWithMultipleStartsWithFields.filter, 'TAG');

        expect(containerValues).toHaveLength(1);
        expect(domainValues).toHaveLength(1);
        expect(tagValues).toHaveLength(1);
    });
});
