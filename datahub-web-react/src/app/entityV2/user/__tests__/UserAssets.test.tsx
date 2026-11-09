import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { UserAssets } from '@app/entityV2/user/UserAssets';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const sectionPropsMock = vi.fn();
vi.mock('@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection', () => ({
    EmbeddedListSearchSection: (props: unknown) => {
        sectionPropsMock(props);
        return null;
    },
}));
vi.mock('@app/entityV2/user/useGetUserGroupUrns', () => ({
    default: () => ({ groupUrns: [], data: {}, loading: false }),
}));

describe('UserAssets', () => {
    it('applies the search bar view', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <UserAssets urn="urn:li:corpuser:datahub" />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(sectionPropsMock).toHaveBeenLastCalledWith(expect.objectContaining({ applyView: true }));
    });
});
