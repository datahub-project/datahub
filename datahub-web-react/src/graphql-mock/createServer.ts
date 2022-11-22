/* eslint-disable global-require */
/* eslint-disable @typescript-eslint/no-var-requires */

if (import.meta.env.VITE_MOCK === 'true') {
    require('./server').makeServer();
} else if (import.meta.env.VITE_MOCK === 'cy') {
    require('./server').makeServerForCypress();
}

export {};
