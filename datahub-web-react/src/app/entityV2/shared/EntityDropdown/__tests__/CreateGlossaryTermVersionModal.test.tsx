import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import EntityContext from '@app/entity/shared/EntityContext';
import CreateGlossaryTermVersionModal from '@app/entityV2/shared/EntityDropdown/CreateGlossaryTermVersionModal';
import { GlossaryEntityContext } from '@app/entityV2/shared/GlossaryEntityContext';
import CustomThemeProvider from '@src/CustomThemeProvider';
import { EntityRegistryContext } from '@src/entityRegistryContext';
import { getTestEntityRegistry } from '@utils/test-utils/TestPageContainer';
import { mockVisibilityObserver } from '@utils/test-utils/mockVisibilityObserver';

import { EntityType, GlossaryNode } from '@types';

const TERM_URN = 'urn:li:glossaryTerm:my_term';
const PARENT_URN = 'urn:li:glossaryNode:my_node';
const createVersion = vi.fn();
const push = vi.fn();
const setUrnsToUpdate = vi.fn();
const setNodeToNewEntity = vi.fn();
let mutationOptions: { update: (cache: { evict: (options: unknown) => void }) => void };

vi.mock('@graphql/glossaryTermVersion.generated', () => ({
    useCreateGlossaryTermVersionMutation: (options: typeof mutationOptions) => {
        mutationOptions = options;
        return [createVersion];
    },
}));
vi.mock('react-router', async (importOriginal) => ({
    ...(await importOriginal<typeof import('react-router')>()),
    useHistory: () => ({ push }),
}));
// The rich-text editor is heavy in jsdom; a textarea stands in for it.
vi.mock('@components/components/Editor', () => ({
    Editor: ({ content, onChange }: { content?: string; onChange?: (v: string) => void }) => (
        <textarea data-testid="definition-editor" value={content} onChange={(e) => onChange?.(e.target.value)} />
    ),
}));

function renderModal(currentVersionLabel: string | null) {
    const onClose = vi.fn();
    render(
        <CustomThemeProvider>
            <EntityRegistryContext.Provider value={getTestEntityRegistry()}>
                <MemoryRouter>
                    <EntityContext.Provider
                        value={{
                            urn: TERM_URN,
                            entityType: EntityType.GlossaryTerm,
                            entityData: {
                                urn: TERM_URN,
                                parentNodes: {
                                    count: 1,
                                    nodes: [{ urn: PARENT_URN, type: EntityType.GlossaryNode } as GlossaryNode],
                                },
                            },
                            loading: false,
                            baseEntity: null,
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <GlossaryEntityContext.Provider
                            value={{
                                isInGlossaryContext: true,
                                entityData: null,
                                setEntityData: vi.fn(),
                                urnsToUpdate: [],
                                setUrnsToUpdate,
                                isSidebarOpen: true,
                                setIsSidebarOpen: vi.fn(),
                                nodeToNewEntity: {},
                                setNodeToNewEntity,
                                nodeToDeletedUrn: {},
                                setNodeToDeletedUrn: vi.fn(),
                            }}
                        >
                            <CreateGlossaryTermVersionModal
                                termUrn={TERM_URN}
                                currentName="Revenue"
                                currentDefinition="Old definition"
                                currentVersionLabel={currentVersionLabel}
                                onClose={onClose}
                            />
                        </GlossaryEntityContext.Provider>
                    </EntityContext.Provider>
                </MemoryRouter>
            </EntityRegistryContext.Provider>
        </CustomThemeProvider>,
    );
    return { onClose };
}

beforeEach(() => {
    vi.clearAllMocks();
    mockVisibilityObserver();
});

describe('CreateGlossaryTermVersionModal', () => {
    it('prefills from the current version and creates the next one under a known id', async () => {
        createVersion.mockResolvedValue({
            data: { createGlossaryTermVersion: { urn: 'urn:li:versionSet:(x,glossaryTerm)' } },
        });
        const { onClose } = renderModal('v2');

        expect(screen.getByDisplayValue('Revenue')).toBeInTheDocument();
        expect(screen.getByDisplayValue('v3')).toBeInTheDocument();
        fireEvent.click(screen.getByTestId('new-version-submit'));

        await waitFor(() => expect(createVersion).toHaveBeenCalled());
        const { input } = createVersion.mock.calls[0][0].variables;
        expect(input).toMatchObject({ previousTermUrn: TERM_URN, name: 'Revenue', version: 'v3' });
        expect(input.id).toBeTruthy();
        expect(onClose).toHaveBeenCalled();
        const newTermUrn = `urn:li:glossaryTerm:${input.id}`;
        expect(push).toHaveBeenCalledWith(getTestEntityRegistry().getEntityUrl(EntityType.GlossaryTerm, newTermUrn));

        // The new term is added to the previous term's node in the sidebar.
        expect(setUrnsToUpdate).toHaveBeenCalledWith([PARENT_URN]);
        const update = setNodeToNewEntity.mock.calls[0][0];
        expect(update({})[PARENT_URN]).toMatchObject({ urn: newTermUrn, properties: { name: 'Revenue' } });

        // The previous term's cached profile is dropped so its version properties refresh.
        const evict = vi.fn();
        mutationOptions.update({ evict });
        expect(evict).toHaveBeenCalledWith({ id: 'ROOT_QUERY', fieldName: 'glossaryTerm', args: { urn: TERM_URN } });
    });

    it('explains that an unversioned term becomes v1 and suggests v2', () => {
        renderModal(null);
        expect(screen.getByText(/becomes version v1/)).toBeInTheDocument();
        expect(screen.getByDisplayValue('v2')).toBeInTheDocument();
    });

    it('sends the custom id and shows the server error when it is taken', async () => {
        createVersion.mockRejectedValue(new Error("A GlossaryTerm with id 'taken_id' already exists."));
        const { onClose } = renderModal('v2');

        fireEvent.click(screen.getByTestId('new-version-advanced'));
        fireEvent.change(screen.getByTestId('new-version-custom-id'), { target: { value: 'taken_id' } });
        fireEvent.click(screen.getByTestId('new-version-submit'));

        expect(await screen.findByText(/already exists/)).toBeInTheDocument();
        expect(createVersion.mock.calls[0][0].variables.input.id).toBe('taken_id');
        expect(onClose).not.toHaveBeenCalled();
        expect(push).not.toHaveBeenCalled();
    });

    it('keeps submit disabled without a version label or with an invalid id', () => {
        renderModal('draft');
        expect(screen.getByTestId('new-version-submit')).toBeDisabled();

        fireEvent.change(screen.getByTestId('new-version-label'), { target: { value: 'v5' } });
        fireEvent.click(screen.getByTestId('new-version-advanced'));
        fireEvent.change(screen.getByTestId('new-version-custom-id'), { target: { value: 'bad id' } });
        expect(screen.getByTestId('new-version-submit')).toBeDisabled();
    });
});
