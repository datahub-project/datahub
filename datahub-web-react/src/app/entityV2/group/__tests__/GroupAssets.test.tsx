import { MockedProvider } from '@apollo/client/testing';
import { render } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import { GroupAssets } from '@app/entityV2/group/GroupAssets';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

const sectionPropsMock = vi.fn();
vi.mock('@app/entityV2/shared/components/styled/search/EmbeddedListSearchSection', () => ({
    EmbeddedListSearchSection: (props: unknown) => {
        sectionPropsMock(props);
        return null;
    },
}));

describe('GroupAssets', () => {
    it('applies the search bar view', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <GroupAssets urn="urn:li:corpGroup:petshop-team" />
                </TestPageContainer>
            </MockedProvider>,
        );

        expect(sectionPropsMock).toHaveBeenLastCalledWith(expect.objectContaining({ applyView: true }));
    });
});
