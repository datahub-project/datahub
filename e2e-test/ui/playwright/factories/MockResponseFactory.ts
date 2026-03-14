export class MockResponseFactory {
  static createSearchResponse(entities: any[], total: number) {
    return {
      search: {
        searchResults: entities.map((entity) => ({
          entity,
          matchedFields: [],
        })),
        total,
      },
    };
  }

  static createDatasetResponse(dataset: any) {
    return {
      dataset,
    };
  }

  static createErrorResponse(message: string, code: string = 'INTERNAL_SERVER_ERROR') {
    return {
      errors: [
        {
          message,
          extensions: {
            code,
          },
        },
      ],
    };
  }

  static createSuccessResponse(data: any) {
    return {
      data,
    };
  }
}
