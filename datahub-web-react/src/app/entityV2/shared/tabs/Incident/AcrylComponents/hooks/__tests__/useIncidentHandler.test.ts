import { act, renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { Mock } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useIncidentHandler } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/hooks/useIncidentHandler';
import { IncidentAction } from '@app/entityV2/shared/tabs/Incident/constant';
import { updateActiveIncidentInCache } from '@app/entityV2/shared/tabs/Incident/incidentUtils';
import { useRaiseIncidentMutation, useUpdateIncidentMutation } from '@src/graphql/mutations.generated';

vi.mock('@src/graphql/mutations.generated', () => ({
    useRaiseIncidentMutation: vi.fn(),
    useUpdateIncidentMutation: vi.fn(),
}));

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));

vi.mock('@app/entityV2/shared/tabs/Incident/incidentUtils', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@app/entityV2/shared/tabs/Incident/incidentUtils')>();
    return {
        ...actual,
        updateActiveIncidentInCache: vi.fn(),
    };
});

vi.mock('@apollo/client', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@apollo/client')>();
    return {
        ...actual,
        useApolloClient: vi.fn(() => ({})),
    };
});

let mockFormValues: Record<string, unknown> = {};

vi.mock('antd', async (importOriginal) => {
    const actual = await importOriginal<typeof import('antd')>();
    return {
        ...actual,
        Form: {
            ...actual.Form,
            useForm: () => [{ getFieldsValue: () => mockFormValues, setFieldsValue: vi.fn() }],
        },
        message: {
            ...actual.message,
            success: vi.fn(),
            error: vi.fn(),
        },
    };
});

const FORM_VALUES_BASE = {
    resourceUrn: 'urn:li:dataset:test',
    resourceUrns: ['urn:li:dataset:test', 'urn:li:dataset:test2'],
    type: 'OPERATIONAL',
    customType: null,
    title: 'Test incident',
    description: 'Test description',
    priority: null,
    status: 'IN_PROGRESS',
    state: 'ACTIVE',
    message: 'in progress',
};

describe('useIncidentHandler', () => {
    const raiseIncidentMutation = vi.fn().mockResolvedValue({ data: { raiseIncident: 'urn:li:incident:new' } });
    const updateIncidentMutation = vi.fn().mockResolvedValue({});

    beforeEach(() => {
        vi.clearAllMocks();

        mockFormValues = { ...FORM_VALUES_BASE };
        (useRaiseIncidentMutation as Mock).mockReturnValue([raiseIncidentMutation]);
        (useUpdateIncidentMutation as Mock).mockReturnValue([updateIncidentMutation]);
        (useEntityData as Mock).mockReturnValue({ urn: undefined });
    });

    function setupHandler(mode: IncidentAction, incidentUrn?: string) {
        const { result } = renderHook(() =>
            useIncidentHandler({
                mode,
                onSubmit: vi.fn(),
                incidentUrn,
                user: { urn: 'urn:li:corpuser:test' },
                assignees: [],
                linkedAssets: [],
                entity: { urn: 'urn:li:dataset:test', entityType: 'DATASET' },
            }),
        );
        return result;
    }

    it('includes resourceUrns when updating an incident so linked assets can be added', async () => {
        const result = setupHandler(IncidentAction.EDIT, 'urn:li:incident:existing');

        await act(async () => {
            await result.current.handleSubmit();
        });

        expect(updateIncidentMutation).toHaveBeenCalledTimes(1);
        const { input } = updateIncidentMutation.mock.calls[0][0].variables;
        expect(input.resourceUrns).toEqual(FORM_VALUES_BASE.resourceUrns);
    });

    it('omits resourceUrn, type, and customType when updating an incident', async () => {
        const result = setupHandler(IncidentAction.EDIT, 'urn:li:incident:existing');

        await act(async () => {
            await result.current.handleSubmit();
        });

        const { input } = updateIncidentMutation.mock.calls[0][0].variables;
        expect(input).not.toHaveProperty('resourceUrn');
        expect(input).not.toHaveProperty('type');
        expect(input).not.toHaveProperty('customType');
        expect(input).not.toHaveProperty('state');
        expect(input).not.toHaveProperty('message');
    });

    it('still includes resourceUrns when creating a new incident', async () => {
        const result = setupHandler(IncidentAction.CREATE);

        await act(async () => {
            await result.current.handleSubmit();
        });

        expect(raiseIncidentMutation).toHaveBeenCalledTimes(1);
        const { input } = raiseIncidentMutation.mock.calls[0][0].variables;
        expect(input.resourceUrns).toEqual(FORM_VALUES_BASE.resourceUrns);
        expect(updateActiveIncidentInCache).toHaveBeenCalled();
    });
});
