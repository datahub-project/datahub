import { createBrowserHistory } from 'history';

import { getTestEntityRegistry } from '../../../../../utils/test-utils/TestPageContainer';
import { Subview } from '../../../../entity/user/Subview';
import { navigateToSubviewUrl } from '../navigateToSubviewUrl';
import { EntityType } from '../../../../../types.generated';

const history = createBrowserHistory();

const entityRegistry = getTestEntityRegistry();

describe('navigateToSubviewUrl', () => {
    beforeEach(() => {
        jest.spyOn(history, 'push');
    });

    it('navigates to the correct url without subviews', () => {
        navigateToSubviewUrl({
            urn: 'test:urn',
            history,
            entityRegistry,
            entityType: EntityType.CorpUser,
        });
        expect(history.push).toHaveBeenCalledWith({ pathname: '/user/test:urn' });
    });

    it('navigates to the correct url with subviews', () => {
        navigateToSubviewUrl({
            urn: 'test:urn',
            subview: Subview.Ownership,
            item: 'dataset',
            history,
            entityRegistry,
            entityType: EntityType.CorpUser,
        });
        expect(history.push).toHaveBeenCalledWith({ pathname: '/user/test:urn/ownership/dataset' });
    });
});
