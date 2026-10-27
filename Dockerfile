FROM node:24.22.1-alpine@sha256:06d7adf3e0ef61a13f1d28682726366f97a15aa1888c934d3ecec86c33918b53

WORKDIR /app
COPY package.json yarn.lock .yarnrc.yml tsconfig.json ./
COPY src ./src

RUN apk add --no-cache tini && \
    corepack enable && \
    yarn && \
    yarn build && \
    yarn cache clean

ENTRYPOINT ["/sbin/tini"]
CMD ["node", "dist/index.js"]
