import { render } from '@testing-library/react';
import React from 'react';
import { ThemeProvider } from 'styled-components';

import UserHeader from '@app/entityV2/user/UserHeader';
import themeV2 from '@conf/theme/themeV2';

describe('UserHeader', () => {
    it('renders name, title, skills, teams and email', () => {
        const { getByText } = render(
            <ThemeProvider theme={themeV2}>
                <UserHeader
                    name="Jane Doe"
                    title="Software Engineer"
                    skills={['Pandas', 'Multivariate Calculus', 'Juggling']}
                    teams={['Product', 'Data Science']}
                    email="jane@datahub.ui"
                />
            </ThemeProvider>,
        );
        expect(getByText('Jane Doe')).toBeInTheDocument();
        expect(getByText('Software Engineer')).toBeInTheDocument();
        expect(getByText('Pandas')).toBeInTheDocument();
        expect(getByText('Juggling')).toBeInTheDocument();
        expect(getByText('Product')).toBeInTheDocument();
        expect(getByText('Data Science')).toBeInTheDocument();
        expect(getByText('jane@datahub.ui')).toBeInTheDocument();
    });
});
