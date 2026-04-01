{% macro create_streamlit_app(app_name, schema_name,stage_name) %}

    CREATE OR REPLACE STREAMLIT {{ target.database }}.{{ schema_name }}.{{ app_name }}
    ROOT_LOCATION = '@{{ target.database }}.{{ schema_name }}.{{ stage_name }}'
    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = '{{ target.warehouse }}';

{% endmacro %}