import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import SidebarLogicalSection from '@app/entityV2/shared/containers/profile/sidebar/Logical/SidebarLogicalSection';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

const useEntityDataMock = vi.fn();
const useAppConfigMock = vi.fn();
vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: () => useEntityDataMock(),
    useRefetch: () => vi.fn(),
}));
vi.mock('@app/useAppConfig', () => ({
    useAppConfig: () => useAppConfigMock(),
}));
const modalPropsMock = vi.fn();
vi.mock('@app/entityV2/shared/components/styled/search/EmbeddedListSearchModal', () => ({
    EmbeddedListSearchModal: (props: unknown) => {
        modalPropsMock(props);
        return null;
    },
}));

function logicalModelEntityData(canEditProperties: boolean, total = 1) {
    return {
        platform: { properties: { logical: true } },
        privileges: { canEditProperties },
        physicalChildren: {
            total,
            relationships: [
                {
                    entity: {
                        urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.child,PROD)',
                        type: EntityType.Dataset,
                    },
                },
            ],
        },
    };
}

function renderSection(entityData: ReturnType<typeof logicalModelEntityData>) {
    useAppConfigMock.mockReturnValue({ config: { featureFlags: { logicalModelsEnabled: true } } });
    useEntityDataMock.mockReturnValue({
        urn: 'urn:li:dataset:(urn:li:dataPlatform:logical,model,PROD)',
        entityType: EntityType.Dataset,
        entityData,
    });
    render(
        <MockedProvider mocks={[]}>
            <TestPageContainer>
                <SidebarLogicalSection />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('SidebarLogicalSection', () => {
    it('shows link/unlink affordances when the user can edit the model', () => {
        renderSection(logicalModelEntityData(true));

        expect(screen.getByTestId('add-physical-child')).toBeInTheDocument();
        expect(screen.getByTestId('unlink-physical-child')).toBeInTheDocument();
    });

    it('shows a read-only children list when the user cannot edit the model', () => {
        renderSection(logicalModelEntityData(false));

        expect(screen.getByTestId('physical-children-list')).toBeInTheDocument();
        expect(screen.queryByTestId('add-physical-child')).not.toBeInTheDocument();
        expect(screen.queryByTestId('unlink-physical-child')).not.toBeInTheDocument();
    });

    it('applies the search bar view to "View all"', () => {
        renderSection(logicalModelEntityData(false, 12));
        fireEvent.click(screen.getByText(/11 more/));

        expect(modalPropsMock).toHaveBeenLastCalledWith(expect.objectContaining({ applyView: true }));
    });
});
