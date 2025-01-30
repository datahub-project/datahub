import React from 'react';
import { render } from '@testing-library/react';
import UserHeader from '../UserHeader';

describe('UserHeader', () => {
    it('renders name, title, skills, teams and email', () => {
        const { getByText } = render(
            <UserHeader
                name="Jane Doe"
                title="Software Engineer"
                skills={['Pandas', 'Multivariate Calculus', 'Juggling']}
                teams={['Product', 'Data Science']}
                email="jane@datahub.ui"
            />,
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
