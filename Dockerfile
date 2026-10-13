FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY server.js ./

ENV PORT=3000
EXPOSE 3000

HEALTHCHECK --interval=5s --timeout=2s --retries=2 \
  CMD wget -qO- http://localhost:3000/health || exit 1

CMD ["node", "server.js"]
