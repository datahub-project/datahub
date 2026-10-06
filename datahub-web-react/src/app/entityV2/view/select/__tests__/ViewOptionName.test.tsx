import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen } from '@testing-library/react';
import React from 'react';

import { ViewOptionName } from '@app/entityV2/view/select/ViewOptionName';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { DataHubView, DataHubViewType, EntityType, LogicalOperator } from '@types';

const view: DataHubView = {
    urn: 'urn:li:dataHubView:test',
    type: EntityType.DatahubView,
    name: 'Test View',
    viewType: DataHubViewType.Personal,
    definition: {
        entityTypes: [],
        filter: { operator: LogicalOperator.And, filters: [] },
    },
};

describe('ViewOptionName', () => {
    it('hands delete to the caller instead of opening a confirmation inside the view select', async () => {
        const onClickDelete = vi.fn();
        render(
            <MockedProvider mocks={[]} addTypename={false}>
                <TestPageContainer>
                    <ViewOptionName
                        name={view.name}
                        type={view.viewType}
                        view={view}
                        visible
                        isOwnedByUser
                        isUserDefault={false}
                        isGlobalDefault={false}
                        onClickDelete={onClickDelete}
                        selectView={() => {}}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );

        fireEvent.mouseEnter(screen.getByTestId('views-table-dropdown'));
        fireEvent.click(await screen.findByTestId('menu-item-delete'));

        expect(onClickDelete).toHaveBeenCalledTimes(1);
        expect(screen.queryByTestId('modal-confirm-button')).not.toBeInTheDocument();
    });
});
