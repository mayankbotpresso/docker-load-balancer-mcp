FROM node:22-alpine
RUN apk add --no-cache docker-cli docker-cli-compose
WORKDIR /app
COPY controller/controller.js ./controller/controller.js
COPY controller/public ./controller/public
ENV LB_PORT=9080 UI_PORT=9081 BACKEND_HOST=host.docker.internal
EXPOSE 9080 9081
CMD ["node", "controller/controller.js"]
