# The simulator image: generates the scenarios' event logs and container logs (`sparksim prepare`), serves
# the scenario portal (`sparksim portal`) and the mock NodeManager log server (services/nm-logs).
# The History Server itself runs from the official apache/spark image (docker-compose.yml).
FROM python:3.11-slim

ARG UV_VERSION=0.11.32
RUN pip install --no-cache-dir "uv==${UV_VERSION}"

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1 \
    PATH=/app/.venv/bin:$PATH \
    SPARKSIM_SCENARIOS=/app/scenarios

WORKDIR /app
# Dependencies first (cached until uv.lock changes). No dev or reference groups: no PySpark in this image.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-default-groups --no-install-project
COPY src ./src
COPY scenarios ./scenarios
COPY services/nm-logs ./services/nm-logs
RUN uv sync --frozen --no-default-groups && python -m compileall -q src

# Generated output lives in named volumes mounted here; they start with this ownership.
RUN useradd --create-home --uid 1000 sparksim \
 && mkdir -p /data/event-logs /data/yarn-logs \
 && chown -R sparksim:sparksim /data
USER sparksim
CMD ["sparksim", "--help"]
