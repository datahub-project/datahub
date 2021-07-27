# Superset `superset`

Extracts:

- List of charts and dashboards

```yml
source:
  type: superset
  config:
    username: user
    password: pass
    provider: db | ldap
    connect_uri: http://localhost:8088
    env: "PROD" # Optional, default is "PROD"
```

See documentation for superset's `/security/login` at https://superset.apache.org/docs/rest-api for more details on superset's login api.

