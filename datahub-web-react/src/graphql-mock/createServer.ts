/* eslint-disable global-require */
/* eslint-disable @typescript-eslint/no-var-requires */

import { makeServer, makeServerForCypress } from './server';

if (import.meta.env.REACT_APP_MOCK === 'true' || import.meta.env.REACT_APP_MOCK === 'cy') {
    if (import.meta.env.REACT_APP_MOCK === 'cy') {
        makeServerForCypress();
    } else {
        makeServer();
    }
}

export {};
