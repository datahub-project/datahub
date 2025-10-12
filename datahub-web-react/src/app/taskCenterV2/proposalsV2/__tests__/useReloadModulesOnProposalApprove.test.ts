import { renderHook } from '@testing-library/react-hooks';
import { vi } from 'vitest';

import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import {
    MinimalActionRequest,
    useReloadModuleOnProposalApprove,
} from '@app/taskCenterV2/proposalsV2/useReloadModulesOnProposalApprove';

import { ActionRequestType, DataHubPageModuleType, EntityType } from '@types';

// Mock the useReloadableContext hook
vi.mock('@app/sharedV2/reloadableContext/hooks/useReloadableContext', () => ({
    useReloadableContext: vi.fn(),
}));

describe('useReloadModuleOnProposalApprove', () => {
    const mockReloadByKeyType = vi.fn();

    beforeEach(() => {
        vi.clearAllMocks();
        (useReloadableContext as any).mockReturnValue({
            reloadByKeyType: mockReloadByKeyType,
        });
        vi.useFakeTimers();
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it('should not reload any modules for unhandled action types', () => {
        const { result } = renderHook(() => useReloadModuleOnProposalApprove());
        const onProposalApproved = result.current;

        const actionRequest: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:1',
            type: ActionRequestType.TagAssociation, // Using a known type that should not trigger a reload
            params: {},
        };

        onProposalApproved([actionRequest]);
        vi.advanceTimersByTime(3000);
        expect(mockReloadByKeyType).not.toHaveBeenCalled();
    });

    it('should reload ChildHierarchy for CreateGlossaryNode with parentNode', () => {
        const { result } = renderHook(() => useReloadModuleOnProposalApprove());
        const onProposalApproved = result.current;

        const actionRequest: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:1',
            type: ActionRequestType.CreateGlossaryNode,
            params: {
                createGlossaryNodeProposal: {
                    glossaryNode: {
                        name: 'Child Node',
                        parentNode: {
                            urn: 'urn:li:glossaryNode:parent',
                            type: EntityType.GlossaryNode,
                        },
                    },
                },
            },
        };

        onProposalApproved([actionRequest]);
        vi.advanceTimersByTime(3000);
        expect(mockReloadByKeyType).toHaveBeenCalledWith([DataHubPageModuleType.ChildHierarchy], 3000);
    });

    it('should not reload ChildHierarchy for CreateGlossaryNode without parentNode', () => {
        const { result } = renderHook(() => useReloadModuleOnProposalApprove());
        const onProposalApproved = result.current;

        const actionRequest: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:1',
            type: ActionRequestType.CreateGlossaryNode,
            params: {
                createGlossaryNodeProposal: {
                    glossaryNode: {
                        name: 'Child Node',
                    },
                },
            },
        };

        onProposalApproved([actionRequest]);
        vi.advanceTimersByTime(3000);
        expect(mockReloadByKeyType).not.toHaveBeenCalled();
    });

    it('should reload ChildHierarchy for CreateGlossaryTerm with parentNode', () => {
        const { result } = renderHook(() => useReloadModuleOnProposalApprove());
        const onProposalApproved = result.current;

        const actionRequest: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:1',
            type: ActionRequestType.CreateGlossaryTerm,
            params: {
                createGlossaryTermProposal: {
                    glossaryTerm: {
                        name: 'Child Term',
                        parentNode: {
                            urn: 'urn:li:glossaryNode:parent',
                            type: EntityType.GlossaryNode,
                        },
                    },
                },
            },
        };

        onProposalApproved([actionRequest]);
        vi.advanceTimersByTime(3000);
        expect(mockReloadByKeyType).toHaveBeenCalledWith([DataHubPageModuleType.ChildHierarchy], 3000);
    });

    it('should not reload ChildHierarchy for CreateGlossaryTerm without parentNode', () => {
        const { result } = renderHook(() => useReloadModuleOnProposalApprove());
        const onProposalApproved = result.current;

        const actionRequest: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:1',
            type: ActionRequestType.CreateGlossaryTerm,
            params: {
                createGlossaryTermProposal: {
                    glossaryTerm: {
                        name: 'Child Term',
                    },
                },
            },
        };

        onProposalApproved([actionRequest]);
        vi.advanceTimersByTime(3000);
        expect(mockReloadByKeyType).not.toHaveBeenCalled();
    });

    it('should reload Assets for DomainAssociation', () => {
        const { result } = renderHook(() => useReloadModuleOnProposalApprove());
        const onProposalApproved = result.current;

        const actionRequest: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:1',
            type: ActionRequestType.DomainAssociation,
            params: {},
        };

        onProposalApproved([actionRequest]);
        vi.advanceTimersByTime(3000);
        expect(mockReloadByKeyType).toHaveBeenCalledWith([DataHubPageModuleType.Assets], 3000);
    });

    it('should reload OwnedAssets for OwnerAssociation', () => {
        const { result } = renderHook(() => useReloadModuleOnProposalApprove());
        const onProposalApproved = result.current;

        const actionRequest: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:1',
            type: ActionRequestType.OwnerAssociation,
            params: {},
        };

        onProposalApproved([actionRequest]);
        vi.advanceTimersByTime(3000);
        expect(mockReloadByKeyType).toHaveBeenCalledWith([DataHubPageModuleType.OwnedAssets], 3000);
    });

    it('should reload Assets for TermAssociation', () => {
        const { result } = renderHook(() => useReloadModuleOnProposalApprove());
        const onProposalApproved = result.current;

        const actionRequest: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:1',
            type: ActionRequestType.TermAssociation,
            params: {},
        };

        onProposalApproved([actionRequest]);
        vi.advanceTimersByTime(3000);
        expect(mockReloadByKeyType).toHaveBeenCalledWith([DataHubPageModuleType.Assets], 3000);
    });

    it('should call reloadByKeyType with correct types and delay for a single action request', () => {
        const { result } = renderHook(() => useReloadModuleOnProposalApprove());
        const onProposalApproved = result.current;

        const actionRequest: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:1',
            type: ActionRequestType.DomainAssociation,
            params: {},
        };

        onProposalApproved([actionRequest]);

        expect(mockReloadByKeyType).toHaveBeenCalled();
        vi.advanceTimersByTime(3000);
        expect(mockReloadByKeyType).toHaveBeenCalledTimes(1);
        expect(mockReloadByKeyType).toHaveBeenCalledWith([DataHubPageModuleType.Assets], 3000);
    });

    it('should call reloadByKeyType with unique types and delay for multiple action requests', () => {
        const { result } = renderHook(() => useReloadModuleOnProposalApprove());
        const onProposalApproved = result.current;

        const actionRequest1: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:1',
            type: ActionRequestType.DomainAssociation,
            params: {},
        };
        const actionRequest2: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:2',
            type: ActionRequestType.OwnerAssociation,
            params: {},
        };
        const actionRequest3: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:3',
            type: ActionRequestType.TermAssociation,
            params: {},
        };
        const actionRequest4: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:4',
            type: ActionRequestType.CreateGlossaryNode,
            params: {
                createGlossaryNodeProposal: {
                    glossaryNode: {
                        name: 'Child Node',
                        parentNode: {
                            urn: 'urn:li:glossaryNode:parent',
                            type: EntityType.GlossaryNode,
                        },
                    },
                },
            },
        };

        onProposalApproved([actionRequest1, actionRequest2, actionRequest3, actionRequest4]);

        expect(mockReloadByKeyType).toHaveBeenCalled();
        vi.advanceTimersByTime(3000);
        expect(mockReloadByKeyType).toHaveBeenCalledTimes(1);
        expect(mockReloadByKeyType).toHaveBeenCalledWith(
            [DataHubPageModuleType.Assets, DataHubPageModuleType.OwnedAssets, DataHubPageModuleType.ChildHierarchy],
            3000,
        );
    });

    it('should not call reloadByKeyType if no module types are identified', () => {
        const { result } = renderHook(() => useReloadModuleOnProposalApprove());
        const onProposalApproved = result.current;

        const actionRequest: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:1',
            type: ActionRequestType.TagAssociation,
            params: {},
        };

        onProposalApproved([actionRequest]);

        vi.advanceTimersByTime(3000);
        expect(mockReloadByKeyType).not.toHaveBeenCalled();
    });

    it('should handle multiple action requests with some resulting in no module types', () => {
        const { result } = renderHook(() => useReloadModuleOnProposalApprove());
        const onProposalApproved = result.current;

        const actionRequest1: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:1',
            type: ActionRequestType.DomainAssociation,
            params: {},
        };
        const actionRequest2: MinimalActionRequest = {
            urn: 'urn:li:actionRequest:2',
            type: ActionRequestType.TagAssociation,
            params: {},
        };

        onProposalApproved([actionRequest1, actionRequest2]);

        expect(mockReloadByKeyType).toHaveBeenCalled();
        vi.advanceTimersByTime(3000);
        expect(mockReloadByKeyType).toHaveBeenCalledTimes(1);
        expect(mockReloadByKeyType).toHaveBeenCalledWith([DataHubPageModuleType.Assets], 3000);
    });
});
