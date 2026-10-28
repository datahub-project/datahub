import { MockedProvider } from '@apollo/client/testing';
import { render, screen } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';

import LinkPhysicalChildModal from '@app/entityV2/shared/logicalModels/LinkPhysicalChildModal';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

describe('LinkPhysicalChildModal', () => {
    it('disables link until a child dataset is selected', () => {
        render(
            <MockedProvider mocks={[]}>
                <TestPageContainer>
                    <LinkPhysicalChildModal
                        logicalParentUrn="urn:li:dataset:(urn:li:dataPlatform:logical,p,PROD)"
                        onClose={() => {}}
                        onLinked={() => {}}
                    />
                </TestPageContainer>
            </MockedProvider>,
        );
        expect(screen.getByTestId('link-physical-child-button')).toBeDisabled();
    });
});
