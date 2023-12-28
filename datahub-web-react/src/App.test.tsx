import React from 'react';
import { act } from 'react-dom/test-utils';
import { render } from '@testing-library/react';
import App from './App';

// eslint-disable-next-line vitest/expect-expect
test('renders the app', async () => {
    await act(() => {
        render(<App />);
    });
});
