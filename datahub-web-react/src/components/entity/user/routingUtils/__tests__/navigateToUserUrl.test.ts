import { createBrowserHistory } from 'history';

import { getTestEntityRegistry } from '../../../../../utils/test-utils/TestPageContainer';
import { Subview } from '../../Subview';
import { navigateToUserUrl } from '../navigateToUserUrl';

const history = createBrowserHistory();

const entityRegistry = getTestEntityRegistry();

describe('navigateToUserUrl', () => {
    beforeEach(() => {
        jest.spyOn(history, 'push');
    });

    it('navigates to the correct url without subviews', () => {
        navigateToUserUrl({
            urn: 'test:urn',
            history,
            entityRegistry,
        });
        expect(history.push).toHaveBeenCalledWith({ pathname: '/user/test:urn' });
    });

    it('navigates to the correct url with subviews', () => {
        navigateToUserUrl({
            urn: 'test:urn',
            subview: Subview.Ownership,
            item: 'dataset',
            history,
            entityRegistry,
        });
        expect(history.push).toHaveBeenCalledWith({ pathname: '/user/test:urn/ownership/dataset' });
    });
});
