// Initialize test data for Neo4j integration test
// Create nodes with various property types
CREATE (u1:User {
  user_id: 1,
  name: "Alice Johnson", 
  email: "alice@example.com",
  age: 30,
  is_active: true,
  created_date: date("2023-01-15"),
  last_login: datetime("2024-01-15T10:30:00Z"),
  tags: ["premium", "verified"]
});

CREATE (u2:User {
  user_id: 2,
  name: "Bob Smith",
  email: "bob@example.com", 
  age: 25,
  is_active: false,
  created_date: date("2023-03-20"),
  last_login: datetime("2024-01-10T14:22:00Z"),
  tags: ["standard"]
});

CREATE (p1:Product {
  product_id: "PROD-001",
  title: "Wireless Headphones",
  price: 199.99,
  in_stock: true,
  category: "Electronics",
  launch_date: date("2023-06-01"),
  specs: ["bluetooth", "noise-canceling", "20hr-battery"]
});

CREATE (p2:Product {
  product_id: "PROD-002", 
  title: "Gaming Keyboard",
  price: 129.99,
  in_stock: false,
  category: "Electronics",
  launch_date: date("2023-08-15"),
  specs: ["mechanical", "rgb", "wireless"]
});

CREATE (o1:Order {
  order_id: "ORD-12345",
  total_amount: 199.99,
  order_date: datetime("2024-01-14T09:15:00Z"),
  status: "completed",
  shipping_address: "123 Main St, City, State"
});

// Create relationships with properties
CREATE (u1)-[:PLACED_ORDER {
  order_date: datetime("2024-01-14T09:15:00Z"),
  payment_method: "credit_card"
}]->(o1);

CREATE (o1)-[:CONTAINS_ITEM {
  quantity: 1,
  unit_price: 199.99,
  discount_applied: 0.0
}]->(p1);

CREATE (u1)-[:VIEWED_PRODUCT {
  view_date: datetime("2024-01-13T15:30:00Z"),
  view_duration_seconds: 45
}]->(p1);

CREATE (u1)-[:VIEWED_PRODUCT {
  view_date: datetime("2024-01-13T16:10:00Z"),
  view_duration_seconds: 30
}]->(p2);

CREATE (u2)-[:VIEWED_PRODUCT {
  view_date: datetime("2024-01-12T11:20:00Z"),
  view_duration_seconds: 60
}]->(p1);

// Create additional relationship types to test variety
CREATE (u1)-[:FOLLOWS]->(u2);
CREATE (p1)-[:SIMILAR_TO {similarity_score: 0.8}]->(p2);