# Hello World MFE - DataHub Micro-Frontend Template

A feature-rich starter template for building custom micro-frontends that integrate with DataHub.

> **📘 For detailed documentation**, see [docs/CUSTOMER_GUIDE.md](docs/CUSTOMER_GUIDE.md)

## Features

This MFE demonstrates:

- **DataHub API Integration** - Calling GraphQL APIs (listRecommendations, me)
- **MFE Internal Routing** - Navigate between pages within the MFE
- **DataHub Navigation** - Link to DataHub entity pages
- **User Context** - Access current user information

## Quick Start

### 1. Install Dependencies

```bash
cd hello-world-mfe
npm install
# or
yarn install
```

### 2. Start Development Server

```bash
npm start
```

Your MFE runs at `http://localhost:3002`

### 3. Configure DataHub

Make sure `datahub-frontend/conf/mfe.config.local.yaml` contains:

```yaml
subNavigationMode: false
microFrontends:
    - id: HelloWorld
      label: Hello World
      path: /helloworld
      remoteEntry: http://localhost:3002/remoteEntry.js
      module: helloWorldMFE/mount
      flags:
          enabled: true
          showInNav: true
      navIcon: HandWaving
```

### 4. Access in DataHub

Navigate to `http://localhost:3000/mfe/helloworld` (or your DataHub URL)

## API Integration Examples

### Calling DataHub GraphQL API

```typescript
// No hostname needed - MFE runs in DataHub's context
const response = await fetch('/api/v2/graphql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
        query: `
            query listRecommendations($input: ListRecommendationsInput!) {
                listRecommendations(input: $input) {
                    modules {
                        title
                        moduleId
                        content {
                            entity {
                                urn
                                type
                            }
                        }
                    }
                }
            }
        `,
        variables: {
            input: {
                userUrn: 'urn:li:corpuser:datahub',
                requestContext: { scenario: 'HOME' },
                limit: 10,
            },
        },
    }),
});
```

### Getting Current User

```typescript
const ME_QUERY = `
    query GetMe {
        me {
            corpUser {
                urn
                username
                properties {
                    displayName
                    email
                }
            }
        }
    }
`;

const response = await fetch('/api/v2/graphql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: ME_QUERY }),
});
```

## Routing

### MFE Internal Routing

Use React state to manage routes within your MFE:

```typescript
const [currentRoute, setCurrentRoute] = useState<'home' | 'details'>('home');

// Navigate within MFE
<button onClick={() => setCurrentRoute('details')}>View Details</button>

// Render based on route
{currentRoute === 'home' && <HomePage />}
{currentRoute === 'details' && <DetailsPage />}
```

### Navigation to DataHub Pages

Navigate to DataHub pages using `window.location`:

```typescript
function navigateToDataHub(path: string) {
    window.location.href = path;
}

// Examples
navigateToDataHub('/search');
navigateToDataHub('/glossary');
navigateToDataHub('/dataset/urn:li:dataset:...');
```

Common DataHub routes:
| Route | Description |
|-------|-------------|
| `/` | Home page |
| `/search` | Search page |
| `/search?query=...` | Search with query |
| `/glossary` | Glossary |
| `/settings` | Settings |
| `/dataset/{urn}` | Dataset profile |
| `/dashboard/{urn}` | Dashboard profile |
| `/chart/{urn}` | Chart profile |

## Project Structure

```
hello-world-mfe/
├── docs/
│   └── CUSTOMER_GUIDE.md   # Detailed documentation
├── public/
│   └── index.html          # HTML template
├── src/
│   ├── App.tsx             # Main app with routing & API calls
│   ├── mount.tsx           # Mount function (DO NOT RENAME)
│   ├── index.tsx           # Standalone entry point
│   └── global.d.ts         # TypeScript declarations
├── package.json
├── tsconfig.json
├── webpack.config.js       # Module Federation config
└── README.md
```

## Customization

### Adding New Pages

1. Create a new component:
```typescript
const MyNewPage: React.FC = () => (
    <div>My New Page Content</div>
);
```

2. Add to route type:
```typescript
type Route = 'home' | 'recommendations' | 'about' | 'myNewPage';
```

3. Add navigation button and render logic:
```typescript
{renderNavButton('myNewPage', '🆕 My Page')}

{currentRoute === 'myNewPage' && <MyNewPage />}
```

### Adding New API Calls

```typescript
const MY_QUERY = `
    query MyQuery($input: MyInput!) {
        myEndpoint(input: $input) {
            field1
            field2
        }
    }
`;

const data = await callGraphQL<MyResponseType>(MY_QUERY, {
    input: { /* variables */ }
});
```

## Available Scripts

| Script | Description |
|--------|-------------|
| `npm start` | Start dev server on port 3002 |
| `npm run build` | Build for production |
| `npm run dev` | Start dev server and open browser |
| `npm run typecheck` | Run TypeScript type checking |
| `npm run clean` | Remove build artifacts |

## Key Points

| Aspect | Details |
|--------|---------|
| **API Calls** | Use relative paths (`/api/v2/graphql`) |
| **Authentication** | Automatic via session cookies |
| **Navigation** | `window.location.href` for DataHub pages |
| **Internal Routing** | React state (no need for react-router) |

## Troubleshooting

| Issue | Solution |
|-------|----------|
| API calls fail | Make sure you're accessing the MFE through DataHub, not directly |
| CORS errors | MFE should be loaded via DataHub, not standalone |
| User not loading | Check that you're logged in to DataHub |
| Recommendations empty | Ensure metadata is ingested and usage tracking is enabled |

## Learn More

- [Detailed Customer Guide](docs/CUSTOMER_GUIDE.md)
- [DataHub GraphQL API](https://datahubproject.io/docs/api/graphql/overview)
- [Webpack Module Federation](https://webpack.js.org/concepts/module-federation/)
