FROM quay.io/astronomer/astro-runtime:11.5.0
COPY profiles.yml /home/astro/.dbt/profiles.yml

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && deactivate