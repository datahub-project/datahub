import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import ManageLineageMenu from '@app/lineageV3/LineageEntityNode/ManageLineageMenu';
import { FetchStatus, LineageEntity } from '@app/lineageV3/common';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType, LineageDirection } from '@types';

vi.mock('../../manualLineage/ManageLineageModal', () => ({
    default: ({ direction }: { direction: LineageDirection }) => (
        <div data-testid="manage-lineage-modal">{direction}</div>
    ),
}));

const URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)';

const node: LineageEntity = {
    id: URN,
    urn: URN,
    type: EntityType.Dataset,
    entity: { urn: URN, type: EntityType.Dataset, name: 'test', canEditLineage: true },
    fetchStatus: {
        [LineageDirection.Upstream]: FetchStatus.COMPLETE,
        [LineageDirection.Downstream]: FetchStatus.COMPLETE,
    },
    isExpanded: {
        [LineageDirection.Upstream]: true,
        [LineageDirection.Downstream]: true,
    },
    filters: {
        [LineageDirection.Upstream]: { facetFilters: new Map() },
        [LineageDirection.Downstream]: { facetFilters: new Map() },
    },
};

function renderMenu(onManageLineage?: (direction: LineageDirection) => void) {
    return render(
        <MockedProvider>
            <TestPageContainer>
                <ManageLineageMenu
                    node={node}
                    refetch={{ [LineageDirection.Upstream]: vi.fn(), [LineageDirection.Downstream]: vi.fn() }}
                    isRootUrn
                    isGhost={false}
                    isOpen
                    setDisplayedMenuNode={vi.fn()}
                    onManageLineage={onManageLineage}
                />
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('ManageLineageMenu', () => {
    it('delegates Edit upstream to onManageLineage instead of opening its own modal', () => {
        const onManageLineage = vi.fn();
        renderMenu(onManageLineage);

        fireEvent.click(screen.getByTestId('edit-upstream-lineage'));

        expect(onManageLineage).toHaveBeenCalledWith(LineageDirection.Upstream);
        expect(screen.queryByTestId('manage-lineage-modal')).not.toBeInTheDocument();
    });

    it('opens its own modal when no onManageLineage is passed', () => {
        renderMenu();

        fireEvent.click(screen.getByTestId('edit-downstream-lineage'));

        expect(screen.getByTestId('manage-lineage-modal')).toHaveTextContent(LineageDirection.Downstream);
    });
});
