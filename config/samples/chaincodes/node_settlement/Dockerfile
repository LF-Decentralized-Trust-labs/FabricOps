FROM node:18-bookworm-slim
LABEL org.opencontainers.image.source="https://github.com/LF-Decentralized-Trust-labs/FabricOps" \
      org.opencontainers.image.description="FabricOps Node settlement CCaaS sample chaincode" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install --omit=dev

COPY index.js ./

EXPOSE 7052

CMD ["npm", "run", "start:ccaas"]
