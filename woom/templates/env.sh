{% if env.raw_text %}
# - raw init
{{ env.raw_text }}
{% endif %}
{% if env.module_load %}
# - environment modules
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
{% if workflow_dir is defined %}
{% set venv_activate = os.path.join(workflow_dir, ".venv", "bin", "activate") %}
{% if env.uv_venv is true or (env.uv_env is none and os.path.exists(venv_activate)) %}
{% endif %}
# - uv virtual environment
source {{ venv_activate }}
{% endif %}
{% if env.has_vars() %}
# - environment variables
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
{% if env.conda_activate %}
# - conda
{% if env.conda_setup %}
{{ env.conda_setup }}
{% endif %}
conda activate {{ env.conda_activate }}
{% endif %}