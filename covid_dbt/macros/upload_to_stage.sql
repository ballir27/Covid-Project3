{% macro upload_to_stage(local_path, schema_name,stage_name) %}

    {# 1. Create the stage (No @ symbol here) #}
    CREATE STAGE IF NOT EXISTS {{ target.database }}.{{ schema_name }}.{{ stage_name }};

    {# 2. Upload the file (The @ must come BEFORE the database name) #}
    PUT file://{{ local_path }} @{{ target.database }}.{{ schema_name }}.{{ stage_name }} 
        auto_compress=false 
        overwrite=true;

{% endmacro %}