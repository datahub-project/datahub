# Docker Compose Profiles - Acryl Addendum

This is in a separate file to avoid complications that may arise from git merges.

To run `docker compose --profile` off acryl-main, you will need to set DATAHUB_REPO to access images built
off the acryl-main branch.

For example: `DATAHUB_REPO="795586375822.dkr.ecr.us-west-2.amazonaws.com" docker compose --profile debug up`
