# CreateNewDomainModal

A modal component for creating new domains with owners and optional parent domain selection.

## Structure

```
CreateNewDomainModal/
├── CreateNewDomainModal.tsx    # Main modal component
├── DomainDetailsSection.tsx    # Domain details form fields
├── types.ts                    # TypeScript type definitions
├── index.ts                    # Export file
└── README.md                   # This file
```

## Components

### CreateNewDomainModal

The main modal component that handles:

- Domain creation logic
- Owner assignment
- Form state management
- Analytics tracking

### DomainDetailsSection

A subcomponent that renders the domain details form fields:

- Domain name (required)
- Domain description
- Parent domain selection (if nested domains enabled)
- Advanced options (custom domain ID)

## Usage

```tsx
import CreateNewDomainModal from '@app/domainV2/CreateNewDomainModal';

const [isModalOpen, setIsModalOpen] = useState(false);

const handleCreateDomain = (
    urn: string,
    id: string | undefined,
    name: string,
    description: string | undefined,
    parentDomain?: string,
) => {
    // Handle the newly created domain
    console.log('Domain created:', { urn, id, name, description, parentDomain });
};

<CreateNewDomainModal open={isModalOpen} onClose={() => setIsModalOpen(false)} onCreate={handleCreateDomain} />;
```

## Features

- **Domain Creation**: Creates domains with name, description, and optional parent
- **Owner Assignment**: Allows adding multiple owners to the domain
- **Parent Domain Selection**: Uses DomainParentSelect for parent domain selection
- **Advanced Options**: Collapsible section for custom domain ID
- **Form Validation**: Ensures required fields are provided
- **Analytics**: Tracks domain creation events
- **Loading States**: Proper loading indicators during creation
- **Error Handling**: Comprehensive error handling and user feedback

## Dependencies

- `@src/alchemy-components` - For Input and TextArea components
- `@app/entityV2/shared/EntityDropdown/DomainParentSelect` - For parent domain selection
- `@app/sharedV2/owners/OwnersSection` - For owner assignment
- Ant Design components for Modal and Collapse
