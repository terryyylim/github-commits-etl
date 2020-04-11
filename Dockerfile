# Derived from official mysql image (our base image)
FROM postgres:12.2-alpine

ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=ghcommits_postgres
