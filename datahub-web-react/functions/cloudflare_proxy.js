/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

// This runs in the Cloudflare Pages Functions environment.
// It proxies requests along to a backend server, so that we can
// use ephemeral preview environments for frontend changes.

/**
 * @type {import('@cloudflare/workers-types').PagesFunction}
 */
export function onRequest(context) {
    // These are set in the Cloudflare dashboard.
    // https://developers.cloudflare.com/pages/functions/bindings/#environment-variables
    const proxyTarget = new URL(context.env.CLOUDFLARE_BACKEND_PROXY_TARGET);

    const { request } = context;
    const url = new URL(request.url);
    url.protocol = proxyTarget.protocol;
    url.hostname = proxyTarget.hostname;
    url.port = proxyTarget.port;
    const newRequest = new Request(url.toString(), {
        method: request.method,
        headers: request.headers,
        body: request.body,
        redirect: request.redirect,
        credentials: request.credentials
    });
    console.log("newRequest", newRequest);
    return fetch(newRequest);
}