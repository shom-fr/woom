{% block raw_text -%}
{% if task_env.raw_text %}
# Raw init env
{{ task_env.raw_text }}
{% endif %}
{% endblock %}
{% block modules -%}
{% if task_env.module_load %}

# Environment modules
{% if task_env.module_setup %}
{{ task_env.module_setup }}
{% endif %}
{% if task_env.module_use %}
module use {{ task_env.module_use }}
{% endif %}
{% if task_env.module_load %}
module load {{ task_env.module_load }}
{% endif %}
{% endif %}
{% endblock %}
{% block uv -%}
{% if workflow_dir is defined %}
{% set venv_activate = os.path.join(workflow_dir, ".venv", "bin", "activate") %}
{% if task_env.uv_venv is true and os.path.exists(venv_activate) %}

# UV virtual environment
source {{ venv_activate }}
{% endif %}
{% endif %}

{% endblock %}
{% block env_vars -%}
{% if task_env.has_vars() %}
# Environment variables
{# forward #}
{% for name in task_env.vars_forward %}
export {{ name }}="{{ os.environ[name] }}"
{% endfor %}
{# set #}
{% for name, value in task_env.vars_set.items() %}
export {{ name }}="{{ value|as_str_env }}"
{% endfor %}
{# prepend #}
{% for name, value in task_env.vars_prepend.items() %}
export {{ name }}={{ value|as_str_env }}{{ os.pathsep }}${{ name }}
{% endfor %}
{# append #}
{% for name, value in task_env.vars_append.items() %}
export {{ name }}=${{ name }}{{ os.pathsep }}{{ value|as_str_env }}
{% endfor %}
{% endif %}
{% endblock %}
{% block conda -%}
{% if task_env.conda_activate %}

# Conda
{% if task_env.conda_setup %}
{{ task_env.conda_setup }}
{% endif %}
conda activate {{ task_env.conda_activate }}
{% endif %}
{% endblock %}

{% block custom -%}
{# Custom configuration block #}
{% endblock %}
