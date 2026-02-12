import React from 'react';

import AuthPageContainer from '@app/auth/shared/AuthPageContainer';
import SignUpModal from '@app/auth/signupV2/SignUpModal';

export default function SignUpV2() {
    return (
        <AuthPageContainer>
            <SignUpModal />
        </AuthPageContainer>
    );
}
