/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { convertStateToUpdateInput, convertStateToView, searchViews } from '@app/entityV2/view/utils';
import { searchViewsMock, viewBuilderStateMock } from '@src/Mocks';

import { EntityType } from '@types';

describe('Entity V2 views utils tests ->', () => {
    it('should convert an instance of the View builder state into the input required to create or update a view in GraphQL', () => {
        const result = convertStateToUpdateInput(viewBuilderStateMock);
        expect(result).toStrictEqual(viewBuilderStateMock);
    });
    it('should convert ViewBuilderState and an URN into a DataHubView object.', () => {
        const mockURN = 'test-urn';
        const result = convertStateToView(mockURN, viewBuilderStateMock);
        const expectedResult = {
            urn: mockURN,
            type: EntityType.DatahubView,
            ...viewBuilderStateMock,
        };
        expect(result).toStrictEqual(expectedResult);
    });
    it('should search through a list of Views by a text string by comparing against View name and descriptions', () => {
        const searchString = 'VIEW_BUILDER';
        const result = searchViews(searchViewsMock, searchString);
        expect(result).toStrictEqual([searchViewsMock[0]]);
    });
});
