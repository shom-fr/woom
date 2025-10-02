{% block raw_text -%}
{% if env.raw_text %}
# Raw init env
{{ env.raw_text }}
{% endif %}
{% endblock %}
{% block modules -%}
{% if env.module_load %}

# Environment modules
{% if env.module_setup %}
{{ env.module_setup }}
{% endif %}
{% if env.module_use %}
module use {{ env.module_use }}
{% endif %}
{% if env.module_load %}
module load {{ env.module_load }}
{% endif %}
{% endif %}
{% endblock %}
{% block uv -%}
{% if workflow_dir is defined %}
{% set venv_activate = os.path.join(workflow_dir, ".venv", "bin", "activate") %}
{% if env.uv_venv is true or (env.uv_env is none and os.path.exists(venv_activate)) %}

# UV virtual environment
source {{ venv_activate }}
{% endif %}
{% endif %}
{% endblock %}
{% block env_vars -%}
{% if env.has_vars() %}
# Environment variables
{# forward #}
{% for name in env.vars_forward %}
export {{ name }}="{{ os.environ[name] }}"
{% endfor %}
{# set #}
{% for name, value in env.vars_set.items() %}
export {{ name }}="{{ value|as_str_env }}"
{% endfor %}
{# prepend #}
{% for name, value in env.vars_prepend.items() %}
export {{ name }}={{ value|as_str_env }}{{ os.pathsep }}${{ name }}
{% endfor %}
{# append #}
{% for name, value in env.vars_append.items() %}
export {{ name }}=${{ name }}{{ os.pathsep }}{{ value|as_str_env }}
{% endfor %}
{% endif %}
{% endblock %}
{% block conda -%}
{% if env.conda_activate %}

# Conda
{% if env.conda_setup %}
{{ env.conda_setup }}
{% endif %}
conda activate {{ env.conda_activate }}
{% endif %}
{% endblock %}

{% block custom -%}
{# Custom configuration block #}
{% endblock %}
