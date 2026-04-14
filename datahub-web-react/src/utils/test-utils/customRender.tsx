import { MockedProvider, MockedProviderProps } from '@apollo/client/testing';
import { RenderOptions, render } from '@testing-library/react';
import React from 'react';

import CustomThemeProvider from '@src/CustomThemeProvider';

// ApolloTestWrapper wraps children in MockedProvider with default settings
const ApolloTestWrapper: React.FC<Partial<MockedProviderProps> & { children: React.ReactNode }> = ({
    mocks = [],
    addTypename = false,
    children,
    ...rest
}) => (
    <MockedProvider mocks={mocks} addTypename={addTypename} {...rest}>
        {children}
    </MockedProvider>
);

// Custom render that always wraps with ApolloTestWrapper and CustomThemeProvider
const customRender = (ui: React.ReactElement, options?: RenderOptions & { apolloMocks?: any[] }) =>
    render(
        <CustomThemeProvider>
            <ApolloTestWrapper mocks={options?.apolloMocks}>{ui}</ApolloTestWrapper>
        </CustomThemeProvider>,
        options,
    );

// Re-export everything from @testing-library/react
export * from '@testing-library/react';
// Override render export
export { customRender as render };
