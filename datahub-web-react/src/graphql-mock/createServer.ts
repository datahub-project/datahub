/* eslint-disable global-require */
/* eslint-disable @typescript-eslint/no-var-requires */

if (import.meta.env.REACT_APP_MOCK === 'true' || import.meta.env.REACT_APP_MOCK === 'cy') {
    if (import.meta.env.REACT_APP_MOCK === 'cy') {
        require('./server').makeServerForCypress();
    } else {
        require('./server').makeServer();
    }
}

export {};
