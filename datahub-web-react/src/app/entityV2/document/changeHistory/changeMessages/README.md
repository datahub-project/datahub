# Document Change Messages

This directory contains the individual components for rendering different types of document changes in the change history timeline.

## Architecture

- **`ChangeMessageComponents.tsx`**: Contains all individual change message components and the main router
- Each change type has its own component for clean separation and maintainability

## Adding a New Change Type

Follow these steps to add support for a new document change type:

### 1. Update Backend GraphQL Schema

First, add your new change type to the enum in `datahub-graphql-core/src/main/resources/knowledge.graphql`:

```graphql
enum DocumentChangeType {
    CREATED
    TITLE_CHANGED
    TEXT_CHANGED
    # ... existing types ...
    MY_NEW_CHANGE_TYPE # Add your new type here
}
```

### 2. Update Backend Event Generator (if needed)

If you need to capture additional data, update `DocumentInfoChangeEventGenerator.java` to include relevant parameters in the change event.

### 3. Create a New Message Component

In `ChangeMessageComponents.tsx`, create a new component:

```typescript
export const MyNewChangeMessage: React.FC<BaseChangeMessageProps> = ({ actorName, details }) => (
    <ActionText>
        <ActorName>{actorName}</ActorName> performed a new action on {details.someField}
    </ActionText>
);
```

**Tips:**

- Use `<ActorName>` for bold text (actor names, titles, etc.)
- Access change details via the `details` prop
- If you need to fetch additional data, use GraphQL hooks (see `ParentChangedMessage` for example)
- If you need user interaction (like "See previous version"), accept an `onAction` prop

### 4. Add to the Router

In the `ChangeMessage` component's switch statement, add your new case:

```typescript
export const ChangeMessage: React.FC<ChangeMessageProps> = ({
    changeType,
    actorName,
    details,
    description,
    onSeeVersion,
}) => {
    switch (changeType) {
        // ... existing cases ...

        case DocumentChangeType.MyNewChangeType:
            return <MyNewChangeMessage actorName={actorName} details={details} />;

        // ... rest of cases ...
    }
};
```

### 5. Update Frontend Types

Run `yarn generate` to regenerate GraphQL types from the schema.

### 6. Test!

1. Create a test document
2. Perform the action that triggers your new change type
3. Open the change history drawer
4. Verify your message displays correctly

## Component Guidelines

### Styling

- Use the provided `ActionText` for the message wrapper
- Use `ActorName` for bold text (names, titles, important values)
- Use `SeeVersionLink` for clickable links

### Message Format

Follow this pattern for consistency:

```
{ActorName} {action verb} {optional object}
```

Examples:

- `John Doe created document`
- `Jane Smith changed title to New Title`
- `Bob Johnson moved document to Marketing Folder`

### Data Fetching

If you need to fetch additional data:

```typescript
const { data } = useGetSomeQuery({
    variables: { urn: details.someUrn },
    skip: !details.someUrn, // Only fetch when needed
});
```

## Examples

### Simple Message (no additional data needed)

```typescript
export const CreatedMessage: React.FC<BaseChangeMessageProps> = ({ actorName }) => (
    <ActionText>
        <ActorName>{actorName}</ActorName> created document
    </ActionText>
);
```

### Message with Details

```typescript
export const TitleChangedMessage: React.FC<BaseChangeMessageProps> = ({ actorName, details }) => (
    <ActionText>
        <ActorName>{actorName}</ActorName> changed title to <ActorName>{details.newTitle}</ActorName>
    </ActionText>
);
```

### Message with Data Fetching

```typescript
export const ParentChangedMessage: React.FC<BaseChangeMessageProps> = ({ actorName, details }) => {
    const { data } = useGetDocumentQuery({
        variables: { urn: details.newParent || '' },
        skip: !details.newParent,
    });

    const parentTitle = data?.document?.info?.title || 'Unknown';

    return (
        <ActionText>
            <ActorName>{actorName}</ActorName> moved document to <ActorName>{parentTitle}</ActorName>
        </ActionText>
    );
};
```

### Message with User Interaction

```typescript
interface MyMessageProps extends BaseChangeMessageProps {
    onAction: () => void;
}

export const MyMessage: React.FC<MyMessageProps> = ({ actorName, onAction }) => (
    <ActionText>
        <ActorName>{actorName}</ActorName> did something.{' '}
        <SeeVersionLink onClick={onAction}>Click here</SeeVersionLink>
    </ActionText>
);
```
