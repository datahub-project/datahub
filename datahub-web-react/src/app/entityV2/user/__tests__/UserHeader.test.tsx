/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { render } from '@testing-library/react';
import React from 'react';

import UserHeader from '@app/entityV2/user/UserHeader';

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
