import * as utils from '@app/entityV2/shared/embed/UpstreamHealth/utils';
import { dataset1, dataset2, dataset3 } from '@src/Mocks';

import { Dataset } from '@types';

// has 1 passing and 1 failing assertion
const dataset1WithAssertions = {
    ...dataset1,
    assertions: {
        count: 2,
        total: 2,
        start: 0,
        assertions: [
            {
                runEvents: {
                    total: 1,
                    failed: 1,
                    succeeded: 0,
                },
            },
            {
                runEvents: {
                    total: 1,
                    failed: 0,
                    succeeded: 1,
                },
            },
        ],
    },
};

// 2 passing assertions
const dataset2WithAssertions = {
    ...dataset2,
    assertions: {
        count: 2,
        total: 2,
        start: 0,
        assertions: [
            {
                runEvents: {
                    total: 1,
                    failed: 0,
                    succeeded: 1,
                },
            },
            {
                runEvents: {
                    total: 1,
                    failed: 0,
                    succeeded: 1,
                },
            },
        ],
    },
};

describe('utils', () => {
    it('should get the number of assertions failing with some passing and some failing', () => {
        const numAssertionsFailing = utils.getNumAssertionsFailing(dataset1WithAssertions as any as Dataset);

        expect(numAssertionsFailing).toBe(1);
    });

    it('should get the number of assertions failing with all passing', () => {
        const numAssertionsFailing = utils.getNumAssertionsFailing(dataset2WithAssertions as any as Dataset);

        expect(numAssertionsFailing).toBe(0);
    });

    it('should get the number of assertions failing with no assertions', () => {
        const numAssertionsFailing = utils.getNumAssertionsFailing(dataset3 as any as Dataset);

        expect(numAssertionsFailing).toBe(0);
    });
});
