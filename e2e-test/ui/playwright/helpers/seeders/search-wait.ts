import type { APIRequestContext } from '@playwright/test';

/**
 * Poll GMS search until an entity written moments ago is visible through the search index.
 * Entity reads by URN are consistent immediately, but anything the UI discovers through search
 * (for example which structured properties to show in the sidebar) lags behind the write.
 */
export async function waitUntilSearchable(
  request: APIRequestContext,
  gmsUrl: string,
  gmsToken: string,
  entityType: string,
  urn: string,
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  const query = `query { searchAcrossEntities(input: { types: [${entityType}], query: "*", start: 0, count: 1,
      orFilters: [{ and: [{ field: "urn", values: ["${urn}"] }] }], searchFlags: { skipCache: true } }) { total } }`;
  let last = '';
  while (Date.now() < deadline) {
    const response = await request.post(`${gmsUrl}/api/graphql`, {
      data: { query },
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${gmsToken}` },
      failOnStatusCode: false,
    });
    last = await response.text();
    if (response.ok()) {
      const body = JSON.parse(last) as { data?: { searchAcrossEntities?: { total?: number } } };
      if ((body.data?.searchAcrossEntities?.total ?? 0) > 0) return;
    }
    await new Promise((resolve) => {
      setTimeout(resolve, 500);
    });
  }
  throw new Error(`${urn} did not become searchable within ${timeoutMs}ms; last response: ${last.slice(0, 300)}`);
}
