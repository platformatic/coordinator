# Build context must be the workspace root (coordinator/).
# See docker-compose.yml in this directory.
FROM node:24-alpine

RUN corepack enable

WORKDIR /workspace

COPY pnpm-workspace.yaml package.json ./
COPY examples/storage-db/package.json examples/storage-db/

RUN --mount=type=cache,target=/root/.local/share/pnpm/store \
    pnpm install

COPY src ./src
COPY tsconfig.json ./
COPY examples/storage-db/src examples/storage-db/src
COPY examples/storage-db/tsconfig.json examples/storage-db/

RUN pnpm --filter @platformatic/coordinator run build

WORKDIR /workspace/examples/storage-db
EXPOSE 3000 8080

CMD ["node", "src/bin/pod.ts"]
