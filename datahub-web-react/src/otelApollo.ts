import { ApolloLink, Observable } from '@apollo/client';
import { SpanStatusCode, context, trace } from '@opentelemetry/api';

/**
 * Apollo link that wraps each GraphQL operation in an OpenTelemetry span named by operation, so
 * traces read `graphql <operationName>` instead of an opaque `POST /api/graphql`, and so a UI query
 * can be found by name. The underlying HTTP fetch span nests under this span because Apollo runs the
 * link chain (and the fetch) synchronously within `context.with` below, so the StackContextManager
 * registered in `otel.ts` has the operation span active when the fetch span is created.
 *
 * The span is marked ERROR when the response carries GraphQL `errors` (which return HTTP 200, so the
 * fetch span alone looks successful) — this also lets the gateway's error-trace sampling policy catch
 * UI errors.
 *
 * Safe when tracing is disabled: the OTel API returns a non-recording span until `initOtel` registers
 * a provider, so every call here is a cheap no-op in that case.
 */
const tracer = trace.getTracer('datahub-web-react-graphql');

export const otelOperationLink = new ApolloLink((operation, forward) => {
    const operationName = operation.operationName || 'anonymous';
    // root: true — each GraphQL operation is its own trace, never nested under a sibling operation
    // that happens to be active when this one starts. The fetch span still nests under this span via
    // context.with below.
    const span = tracer.startSpan(`graphql ${operationName}`, {
        root: true,
        attributes: {
            'graphql.operation.name': operationName,
            // Which page/resource issued this query — DataHub page URLs carry entity URNs / search
            // terms (non-sensitive internal identifiers), captured in full.
            'page.path': window.location.pathname,
            'page.url': window.location.href,
            ...(window.location.search ? { 'page.params': window.location.search } : {}),
        },
    });
    const ctx = trace.setSpan(context.active(), span);

    // End exactly once — on complete/error, or on teardown (unsubscribe from unmount, route change,
    // Apollo cancellation, or a never-completing subscription). Without the teardown path the span
    // would never end → never export → leak in the BatchSpanProcessor queue.
    let ended = false;
    const end = () => {
        if (!ended) {
            ended = true;
            span.end();
        }
    };

    return new Observable((observer) => {
        const subscription = context.with(ctx, () =>
            forward(operation).subscribe({
                next: (result) => {
                    if (result.errors?.length) {
                        span.setStatus({ code: SpanStatusCode.ERROR });
                        span.setAttribute('graphql.errors.count', result.errors.length);
                        // Error code only (not the message) — codes aren't PII; messages can be.
                        const code = result.errors[0]?.extensions?.code;
                        if (code != null) {
                            span.setAttribute('graphql.error.code', String(code));
                        }
                    }
                    observer.next(result);
                },
                error: (err) => {
                    span.setStatus({ code: SpanStatusCode.ERROR, message: String(err?.message ?? err) });
                    end();
                    observer.error(err);
                },
                complete: () => {
                    end();
                    observer.complete();
                },
            }),
        );
        return () => {
            end();
            subscription.unsubscribe();
        };
    });
});
