import type { Page } from '@playwright/test';

type GraphQLPayload = {
  data?: Record<string, unknown>;
  errors?: unknown;
};

export async function executeGraphQL(
  page: Page,
  query: string,
  variables?: Record<string, unknown>,
): Promise<GraphQLPayload> {
  const response = await page.request.post('/api/v2/graphql', {
    data: { query, variables: variables ?? {} },
    headers: { 'Content-Type': 'application/json' },
  });

  if (!response.ok()) {
    throw new Error(`GraphQL request failed: ${response.status()} ${response.statusText()}`);
  }

  const json = (await response.json()) as GraphQLPayload;
  if (json.errors) {
    throw new Error(`GraphQL errors: ${JSON.stringify(json.errors)}`);
  }
  return json;
}

export async function createDocument(
  page: Page,
  id: string,
  title: string,
  text = 'Parent for import tests',
): Promise<string> {
  const result = await executeGraphQL(
    page,
    `mutation CreateDoc($input: CreateDocumentInput!) {
      createDocument(input: $input)
    }`,
    {
      input: {
        id,
        title,
        contents: { text },
      },
    },
  );

  const urn = result.data?.createDocument;
  if (typeof urn !== 'string') {
    throw new Error(`createDocument did not return a URN: ${JSON.stringify(result)}`);
  }
  return urn;
}

export type ImportDocumentsFromFilesResult = {
  createdCount: number;
  updatedCount: number;
  failedCount: number;
  documentUrns: string[];
};

export async function waitForImportDocumentsFromFiles(page: Page): Promise<ImportDocumentsFromFilesResult> {
  const response = await page.waitForResponse(
    (res) => {
      if (!res.url().includes('graphql') || res.request().method() !== 'POST') {
        return false;
      }
      return (res.request().postData() ?? '').includes('importDocumentsFromFiles');
    },
    { timeout: 120_000 },
  );

  const json = (await response.json()) as GraphQLPayload;
  const payload = json.data?.importDocumentsFromFiles as ImportDocumentsFromFilesResult | undefined;
  if (!payload) {
    throw new Error(`importDocumentsFromFiles missing in response: ${JSON.stringify(json)}`);
  }
  return payload;
}

export async function getDocumentParentUrn(page: Page, documentUrn: string): Promise<string | null> {
  const result = await executeGraphQL(
    page,
    `query GetDoc($urn: String!) {
      document(urn: $urn) {
        info {
          parentDocument { document { urn } }
        }
      }
    }`,
    { urn: documentUrn },
  );

  const document = result.data?.document as { info?: { parentDocument?: { document?: { urn?: string } } } } | undefined;
  return document?.info?.parentDocument?.document?.urn ?? null;
}

export async function getDocumentSourceType(page: Page, documentUrn: string): Promise<string> {
  const result = await executeGraphQL(
    page,
    `query GetDoc($urn: String!) {
      document(urn: $urn) {
        info {
          source { sourceType }
          contents { text }
        }
      }
    }`,
    { urn: documentUrn },
  );

  const document = result.data?.document as
    | { info?: { source?: { sourceType?: string }; contents?: { text?: string } } }
    | undefined;
  if (!document?.info?.source?.sourceType) {
    throw new Error(`Document not found or missing source: ${JSON.stringify(result)}`);
  }
  return document.info.source.sourceType;
}
