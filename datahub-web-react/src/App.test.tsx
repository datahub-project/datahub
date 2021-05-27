import React from 'react';
import { act } from 'react-dom/test-utils';
import { render } from '@testing-library/react';
import App from './App';

// eslint-disable-next-line jest/expect-expect
test('renders the app', async () => {
    const promise = Promise.resolve();
    render(<App />);
    await act(() => promise);
});
