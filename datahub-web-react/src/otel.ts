import { SpanStatusCode } from '@opentelemetry/api';
import { ZoneContextManager } from '@opentelemetry/context-zone';
import { W3CTraceContextPropagator } from '@opentelemetry/core';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { registerInstrumentations } from '@opentelemetry/instrumentation';
import { DocumentLoadInstrumentation } from '@opentelemetry/instrumentation-document-load';
import { FetchInstrumentation } from '@opentelemetry/instrumentation-fetch';
import { XMLHttpRequestInstrumentation } from '@opentelemetry/instrumentation-xml-http-request';
import { Resource } from '@opentelemetry/resources';
import { AlwaysOnSampler, BatchSpanProcessor, WebTracerProvider } from '@opentelemetry/sdk-trace-web';
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions';
import { getCLS, getFCP, getFID, getLCP, getTTFB } from 'web-vitals';

import { resolveRuntimePath } from '@utils/runtimeBasePath';

/**
 * Browser-side OpenTelemetry tracing. Emits W3C `traceparent` on same-origin API
 * calls so browser spans correlate with GMS spans in the backend collector (Jaeger).
 *
 * Design constraints (must not lag the UI):
 * - BatchSpanProcessor buffers spans and flushes asynchronously on a timer/size threshold.
 *   Span creation is synchronous but ~microseconds; network export never blocks the main thread.
 * - Spans are exported via a SAME-ORIGIN proxy route on the Play frontend (`/otel/v1/traces`),
 *   which forwards to the internal collector. Keeps the collector private, avoids CORS, and
 *   reuses the existing session — no separate browser auth.
 *
 * Enablement is driven by the `browserTracingEnabled` feature flag (appConfig.featureFlags), passed
 * in by the caller once app config resolves. When disabled, this module is a no-op and adds zero
 * runtime overhead.
 */

// Path of the same-origin OTLP proxy on the Play frontend. Excluded from instrumentation
// to avoid a self-tracing feedback loop.
const OTLP_TRACES_PATH = '/otel/v1/traces';

// BatchSpanProcessor tuning. scheduledDelay is deliberately SHORT (1s): browser spans travel
// browser -> proxy -> forwarder and arrive later than the java agent's near-instant gRPC export. The
// gateway's tail-sampling decision_wait buffers each trace only briefly before finalizing, so a long
// flush delay makes the browser span miss that window — the trace seals with the backend spans but
// without the browser span ("some come, some don't"). 1s keeps browser spans inside the window while
// still batching. Export stays async — never blocks the UI.
const MAX_QUEUE_SIZE = 2048;
const MAX_EXPORT_BATCH_SIZE = 512;
const SCHEDULED_DELAY_MILLIS = 1000;
const EXPORT_TIMEOUT_MILLIS = 30000;

// The browser emits 100% of spans (AlwaysOnSampler). The observe gateway does whole-trace tail
// sampling downstream; a browser span is kept because its trace's Play/GMS spans carry the
// datahub.trace.* resource attribute the gateway keeps on (stamped server-side by the java agent,
// not here). Client-side head sampling is deliberately absent — it would pre-drop spans the tail
// sampler needs to assemble the full trace.

export type BrowserOtelConfig = {
    enabled: boolean;
    // Emit Core Web Vitals as spans. Gated separately from `enabled` (browserWebVitalsEnabled FF) so
    // vitals stay off while request tracing is validated.
    webVitalsEnabled?: boolean;
    serviceName?: string;
    serviceVersion?: string;
};

let initialized = false;

/**
 * Strips query strings and fragments from a URL, keeping only origin + path. Query strings on
 * DataHub API calls can contain tokens or sensitive filters; we do not want those in span data.
 * Exported for testing.
 */
export function redactUrl(url: string): string {
    try {
        const parsed = new URL(url, window.location.origin);
        return `${parsed.origin}${parsed.pathname}`;
    } catch {
        return url.split('?')[0];
    }
}

/**
 * Overwrites URL-bearing span attributes with a query-stripped value. Sets both the legacy
 * (`http.url`) and current (`url.full`) semantic-convention keys so the redaction holds regardless
 * of the instrumentation's semconv version.
 */
function redactSpanUrl(span: { setAttribute: (key: string, value: string) => unknown }, url: string | undefined): void {
    // Placeholder when the URL can't be recovered (failed/aborted request) so the instrumentation's
    // raw full URL — which may carry tokens or filters — never survives on the span.
    const clean = url ? redactUrl(url) : '[redacted]';
    span.setAttribute('http.url', clean);
    span.setAttribute('url.full', clean);
}

/** Resolves the request URL from the fetch input (Request or string) or the response, if present. */
function fetchRequestUrl(request: Request | RequestInit | string, response?: Response): string | undefined {
    if (request instanceof Request) return request.url;
    if (typeof request === 'string') return request;
    return response?.url || undefined;
}

type AnnotatableSpan = {
    setAttribute: (key: string, value: string) => unknown;
    setStatus: (status: { code: SpanStatusCode }) => unknown;
};

/**
 * Tags the current browser page on a span: `page.path` (e.g. /dataset/urn:li:...), `page.url` (full),
 * and `page.params` (query string). Unlike the API fetch URL (kept query-stripped — it can carry
 * tokens), the page location is captured in full: DataHub page URLs carry entity URNs and search
 * terms, treated as non-sensitive internal identifiers, and they show which resource a failing
 * request relates to.
 */
export function setPageAttributes(span: { setAttribute: (key: string, value: string) => unknown }): void {
    span.setAttribute('page.path', window.location.pathname);
    span.setAttribute('page.url', window.location.href);
    if (window.location.search) {
        span.setAttribute('page.params', window.location.search);
    }
}

/**
 * Enriches an HTTP span: redacts the API URL, tags the current browser page, and marks the span
 * ERROR on an HTTP >= 400 status (the instrumentation leaves status unset).
 */
function annotateHttpSpan(span: AnnotatableSpan, url: string | undefined, statusCode: number | undefined): void {
    redactSpanUrl(span, url);
    setPageAttributes(span);
    if (typeof statusCode === 'number' && statusCode >= 400) {
        span.setStatus({ code: SpanStatusCode.ERROR });
    }
}

/**
 * Emits Core Web Vitals (CLS/FCP/FID/LCP/TTFB) as one span each — RUM page-quality signals, not just
 * API traces. These are standalone (browser-only) traces, so the gateway keeps them only once the
 * `datahub.trace.browser` browser-sample policy is deployed; until then they export but are dropped.
 */
function reportWebVitals(provider: WebTracerProvider): void {
    const tracer = provider.getTracer('datahub-web-react-vitals');
    const emit = (metric: { name: string; value: number; id: string }) => {
        const span = tracer.startSpan(`web-vital ${metric.name}`);
        span.setAttribute('web_vital.name', metric.name);
        span.setAttribute('web_vital.value', metric.value);
        span.setAttribute('web_vital.id', metric.id);
        setPageAttributes(span);
        span.end();
    };
    getCLS(emit);
    getFCP(emit);
    getFID(emit);
    getLCP(emit);
    getTTFB(emit);
}

const SESSION_ID_KEY = 'datahub.otel.session_id';

/** Stable per-browser-session id (random, no PII) so all traces from one session can be grouped. */
function sessionId(): string {
    try {
        let id = window.sessionStorage.getItem(SESSION_ID_KEY);
        if (!id) {
            id = crypto.randomUUID();
            window.sessionStorage.setItem(SESSION_ID_KEY, id);
        }
        return id;
    } catch {
        // sessionStorage throws in private-mode / blocked-cookie browsers — use an ephemeral id.
        return crypto.randomUUID();
    }
}

/** Extra resource attributes stamped on every browser span: session grouping + network class. */
function browserResourceAttributes(): Record<string, string> {
    const attrs: Record<string, string> = { 'session.id': sessionId() };
    const effectiveType = (navigator as Navigator & { connection?: { effectiveType?: string } }).connection
        ?.effectiveType;
    if (effectiveType) {
        attrs['network.effective_type'] = effectiveType;
    }
    return attrs;
}

/**
 * Initializes browser tracing when enabled via the browserTracingEnabled feature flag. Safe to call
 * repeatedly (e.g. as app config resolves); only the first enabled call takes effect. No-op when
 * tracing is disabled.
 */
export function initOtel(otelConfig: BrowserOtelConfig): void {
    if (initialized) return;
    if (!otelConfig?.enabled) return;
    initialized = true;

    // Only propagate trace headers to same-origin calls (GraphQL/REST proxied through the frontend).
    // OTEL matches a plain string entry as a URL prefix, so the origin string is sufficient.
    const sameOrigin = window.location.origin;
    // OTLPTraceExporter requires an absolute URL; resolveRuntimePath returns an origin-relative path.
    const exportUrl = new URL(resolveRuntimePath(OTLP_TRACES_PATH), sameOrigin).toString();

    const provider = new WebTracerProvider({
        resource: new Resource({
            [ATTR_SERVICE_NAME]: otelConfig.serviceName ?? 'datahub-frontend-browser',
            [ATTR_SERVICE_VERSION]: otelConfig.serviceVersion ?? 'unknown',
            // Set explicitly so the collector/forwarder doesn't stamp its own namespace (e.g. observe).
            'service.namespace': 'datahub',
            ...browserResourceAttributes(),
        }),
        sampler: new AlwaysOnSampler(),
        spanProcessors: [
            new BatchSpanProcessor(new OTLPTraceExporter({ url: exportUrl }), {
                maxQueueSize: MAX_QUEUE_SIZE,
                maxExportBatchSize: MAX_EXPORT_BATCH_SIZE,
                scheduledDelayMillis: SCHEDULED_DELAY_MILLIS,
                exportTimeoutMillis: EXPORT_TIMEOUT_MILLIS,
            }),
        ],
    });

    provider.register({
        contextManager: new ZoneContextManager(),
        propagator: new W3CTraceContextPropagator(),
    });

    registerInstrumentations({
        instrumentations: [
            new DocumentLoadInstrumentation(),
            new FetchInstrumentation({
                propagateTraceHeaderCorsUrls: [sameOrigin],
                ignoreUrls: [new RegExp(OTLP_TRACES_PATH)],
                applyCustomAttributesOnSpan: (span, request, result) => {
                    // Redact URL (even when none is derivable on a failed fetch), tag route, flag 4xx/5xx.
                    const response = result as Response | undefined;
                    annotateHttpSpan(span, fetchRequestUrl(request, response), response?.status);
                },
            }),
            new XMLHttpRequestInstrumentation({
                propagateTraceHeaderCorsUrls: [sameOrigin],
                ignoreUrls: [new RegExp(OTLP_TRACES_PATH)],
                applyCustomAttributesOnSpan: (span, xhr) => {
                    // responseURL is empty on aborted/failed XHRs → placeholder overwrites any full URL.
                    annotateHttpSpan(span, xhr.responseURL || undefined, xhr.status);
                },
            }),
        ],
    });

    // Flush buffered spans when the tab is hidden or closing, so late spans are not lost.
    const flush = () => {
        provider.forceFlush().catch(() => {
            /* best-effort flush on unload; nothing actionable on failure */
        });
    };
    window.addEventListener('pagehide', flush);
    window.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'hidden') flush();
    });

    if (otelConfig.webVitalsEnabled) {
        reportWebVitals(provider);
    }
}
