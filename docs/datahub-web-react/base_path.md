# Base Path Support in DataHub Frontend

DataHub's React frontend supports runtime base path detection, allowing the same build to be deployed at different URL paths (e.g., `/` or `/datahub/`) without requiring rebuild or reconfiguration.

## How It Works

The base path is determined through a hierarchical detection system:

1. **Server Template** (Primary): The Play Framework backend injects the base path into the HTML template
2. **Config Endpoint** (Fallback): Attempts to fetch base path from `/config` or `config` endpoints
3. **Default** (Last Resort): Falls back to root path `/`

## Using Base Paths in Code

### For Asset URLs

Use the `resolveRuntimePath()` function to resolve any asset path:

```typescript
import { resolveRuntimePath } from '../utils/runtimeBasePath';

// Resolve asset paths
const logoUrl = resolveRuntimePath('/assets/logo.png');
const apiUrl = resolveRuntimePath('/api/graphql');

// Use in components
<img src={resolveRuntimePath('/assets/icons/favicon.ico')} alt="DataHub" />
```

### For Navigation Links

Use React Router's relative paths or resolve absolute paths:

```typescript
import { resolveRuntimePath } from '../utils/runtimeBasePath';

// For React Router navigation (preferred - automatic)
<Link to="/datasets">Datasets</Link>

// For absolute URLs when needed
<a href={resolveRuntimePath('/browse')}>Browse</a>
```

### For API Endpoints

```typescript
import { resolveRuntimePath } from '../utils/runtimeBasePath';

// Resolve API endpoints
const endpoint = resolveRuntimePath('/api/v2/graphql');
fetch(endpoint, { ... });
```

## Configuration

### Server Configuration

The base path is configured in the backend Play application:

```hocon
# datahub-frontend/conf/application.conf
datahub.basePath = "/datahub"
```

### Environment Variables

For containerized deployments:

```bash
# Docker/Kubernetes
DATAHUB_BASE_PATH=/datahub
```

## Examples

### Development

```bash
# Serve at root
yarn start  # Available at http://localhost:3000/

# Serve at subpath
yarn preview --base /datahub  # Available at http://localhost:3000/datahub/
```

### Production Deployment

**At Root Path:**

```bash
DATAHUB_BASE_PATH="" docker run datahub-frontend
# Accessible at: https://datahub.company.com/
```

or unset

```
unset DATAHUB_BASE_PATH
docker run datahub-frontend
```

**At Subpath:**

```bash
DATAHUB_BASE_PATH=/datahub docker run datahub-frontend
# Accessible at: https://company.com/datahub/
```

## Browser Support

The implementation uses standard HTML `<base>` tags and modern JavaScript features:

- All modern browsers (Chrome 60+, Firefox 60+, Safari 12+, Edge 79+)
- Progressive enhancement with fallback detection

## Troubleshooting

### Assets Not Loading

1. Check browser console for 404 errors
2. Verify `window.__DATAHUB_BASE_PATH__` is set correctly
3. Ensure all asset references use `resolveRuntimePath()`

### Incorrect Redirects

1. Check that authentication endpoints use resolved paths
2. Verify React Router basename configuration
3. Test config endpoint accessibility

### Development Issues

```bash
# Clear cache and rebuild
yarn clean
yarn build

# Check base path detection
console.log(window.__DATAHUB_BASE_PATH__);
```

## Implementation Details

- **HTML Template**: `datahub-frontend/app/views/index.scala.html`
- **Runtime Utility**: `src/utils/runtimeBasePath.ts`
- **Asset Resolution**: All `src/` files using `resolveRuntimePath()`
- **Server Handler**: `datahub-frontend/app/controllers/Application.java`

The system automatically handles:

- Favicon and manifest paths
- Main application JS/CSS assets
- Platform logos and theme assets
- API endpoint resolution
- Authentication redirects
