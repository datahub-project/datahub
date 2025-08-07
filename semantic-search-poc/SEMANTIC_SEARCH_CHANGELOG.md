# Semantic Search Implementation Changelog

## 2025-08-27 (Update): GraphQL Resolver Package Reorganization

### Summary

Reorganized GraphQL resolvers to create cleaner fork boundaries by moving semantic search resolvers to a dedicated package.

### Changes Made

- **Created new package**: `com.linkedin.datahub.graphql.resolvers.semantic`
- **Moved 4 files** from `.resolvers.search` to `.resolvers.semantic`:
  - `SemanticSearchResolver.java`
  - `SemanticSearchAcrossEntitiesResolver.java`
  - `SemanticSearchResolverTest.java`
  - `SemanticSearchAcrossEntitiesResolverTest.java`
- **Updated imports**:
  - `AcrylGraphQLPlugin.java` - updated resolver imports
  - `SemanticSearchAcrossEntitiesResolver.java` - added explicit SearchUtils import
- **Removed duplicate**: Deleted `SemanticSearchDisabledException.java` from metadata-io (kept the one in restli-client-api)

### Rationale

- Creates clear separation between keyword search (`.search`) and semantic search (`.semantic`) packages
- Reduces potential merge conflicts with upstream DataHub
- Makes fork-specific code easier to identify and maintain
- Follows the hybrid approach: keeps services together (they're peers), separates GraphQL concerns

### Verification

✅ All code compiles successfully  
✅ All semantic search tests pass  
✅ No files left in original search package

## 2025-08-27: Architecture Revision - Dedicated Endpoints

### Summary

Completed major architectural revision of semantic search implementation, moving from searchMode-based routing to dedicated semantic search endpoints.

### Motivation

The original implementation using `searchMode` field in `SearchFlags` caused frequent merge conflicts with upstream DataHub. The new approach provides complete separation between keyword and semantic search implementations.

### Changes Made

#### 🏗️ **New Components**

1. **SemanticSearchService** (`metadata-io/src/main/java/com/linkedin/metadata/search/SemanticSearchService.java`)

   - Dedicated service handling all semantic search operations
   - Contains logic previously mixed into SearchService
   - Clean interface for semantic-only operations

2. **GraphQL Resolvers**

   - `SemanticSearchResolver` - Single entity type semantic search
   - `SemanticSearchAcrossEntitiesResolver` - Multi-entity semantic search

3. **GraphQL Schema** (`datahub-graphql-core/src/main/resources/semantic-search.acryl.graphql`)
   - New dedicated endpoints: `semanticSearch` and `semanticSearchAcrossEntities`
   - No searchMode field required

#### 🧹 **Cleanup Performed**

1. **SearchService**

   - Removed all semantic search logic (~143 lines)
   - Removed `_searchSemanticAcrossEntities()` method
   - Removed `SemanticEntitySearch` dependency
   - Now handles keyword search exclusively

2. **Schema Cleanup**

   - Removed `searchMode` field from SearchFlags (GraphQL & PDL)
   - Deleted `SearchMode.pdl` enum file completely
   - Updated RestLI snapshot files
   - Cleaned documentation GraphQL schema

3. **Test Updates**
   - Created comprehensive unit tests for new resolvers
   - Updated integration tests to use new endpoints
   - Removed obsolete searchMode routing tests

#### 📝 **Documentation Updates**

- Updated `docs/tech-spec-semantic-search.md` with architecture changes
- Updated POC documentation files with implementation notes
- Created this changelog for historical reference

### Benefits Achieved

✅ **Zero merge conflicts** - No modifications to upstream files  
✅ **Clean separation** - Semantic and keyword search completely isolated  
✅ **Better maintainability** - Each search type can evolve independently  
✅ **Simpler debugging** - Clear code boundaries  
✅ **Performance** - No routing overhead

### Migration Guide

#### For API Consumers

**Before (Obsolete):**

```graphql
query {
  searchAcrossEntities(
    input: {
      query: "customer data"
      searchFlags: { searchMode: SEMANTIC }
    }
  ) {
    searchResults { ... }
  }
}
```

**After (Current):**

```graphql
query {
  semanticSearchAcrossEntities(
    input: {
      query: "customer data"
    }
  ) {
    searchResults { ... }
  }
}
```

#### For Developers

- Use `SemanticSearchService` for any semantic search operations
- Use `SearchService` for keyword search only
- No searchMode checks or routing logic needed
- Each service has its own clear responsibilities

### Testing Validation

- ✅ All unit tests passing
- ✅ Integration tests updated and passing
- ✅ End-to-end GraphQL endpoints tested successfully
- ✅ Local deployment validated
- ✅ POC scripts updated and working

### Future Considerations

- Hybrid search (combining keyword + semantic) would be implemented in semantic endpoints
- Each search type can be optimized independently
- UI can clearly present both search options to users
- Performance monitoring can be done per search type

---

## Previous Implementation Notes

### Original Design (Deprecated)

- Used `SearchMode` enum (KEYWORD, SEMANTIC)
- Single endpoints with mode switching
- Routing logic in SearchService
- Required modifications to core DataHub schemas

### Why We Changed

1. **Merge Conflicts**: Core schema modifications conflicted with upstream
2. **Code Complexity**: Mixed concerns in SearchService
3. **Testing Difficulty**: Hard to test semantic logic in isolation
4. **Performance**: Routing overhead on every search request

---

## 2024-01-27: Removed searchType from Metadata

---

_For technical details, see the implementation plan in `semantic-search-endpoints-plan.md`_
