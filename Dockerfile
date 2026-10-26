FROM node:24-alpine

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
