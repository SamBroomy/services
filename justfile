
dev:
    poetry lock
    poetry install --no-root

unstructured:
    docker run -p 8000:8000 -d --rm --name unstructured-api downloads.unstructured.io/unstructured-io/unstructured-api:latest --port 8000 --host 0.0.0.0

vector_db:
    docker run -p 6333:6333 -p 6334:6334 \
    -v $(pwd)/qdrant_storage:/qdrant/storage:z \
    qdrant/qdrant

compose:
    docker compose up -d

down:
    docker compose down

compose_logs:
    docker compose logs -f

running:
    docker ps

code2prompt:
    code2prompt ./ -e lock,env,drawio --exclude-files env.yml --exclude-folders tests,qdrant_data
