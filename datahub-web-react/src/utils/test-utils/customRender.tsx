/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { MockedProvider, MockedProviderProps } from '@apollo/client/testing';
import { RenderOptions, render } from '@testing-library/react';
import React from 'react';

// ApolloTestWrapper wraps children in MockedProvider with default settings
export const ApolloTestWrapper: React.FC<Partial<MockedProviderProps> & { children: React.ReactNode }> = ({
    mocks = [],
    addTypename = false,
    children,
    ...rest
}) => (
    <MockedProvider mocks={mocks} addTypename={addTypename} {...rest}>
        {children}
    </MockedProvider>
);

// Custom render that always wraps with ApolloTestWrapper
const customRender = (ui: React.ReactElement, options?: RenderOptions & { apolloMocks?: any[] }) =>
    render(<ApolloTestWrapper mocks={options?.apolloMocks}>{ui}</ApolloTestWrapper>, options);

// Re-export everything from @testing-library/react
export * from '@testing-library/react';
// Override render export
export { customRender as render };
