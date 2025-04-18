import { searchViewsMock, viewBuilderStateMock } from '../../../../Mocks';
import { EntityType } from '../../../../types.generated';
import { convertStateToUpdateInput, convertStateToView, searchViews } from '../utils';

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
