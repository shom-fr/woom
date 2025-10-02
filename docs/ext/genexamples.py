"""Generate rst files for examples"""

import logging
import os
from pathlib import Path

from jinja2 import ChoiceLoader, Environment, FileSystemLoader

logger = logging.getLogger(__name__)


def genexamples(app):
    # Main directories
    srcdir = Path(app.env.srcdir)
    templates_dir = srcdir / '_templates' / 'genexamples'
    input_dir = srcdir.parent / "examples"

    # Jinja setup
    loader = ChoiceLoader(
        [
            FileSystemLoader(str(input_dir)),
            FileSystemLoader(str(templates_dir)),
        ]
    )
    jinja_env = Environment(
        loader=loader,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
        autoescape=True,
    )

    # Loop on examples
    examples = {}
    for section in "academic", "realistic":
        section_dir = input_dir / section
        examples[section] = []
        if not section_dir.exists():
            continue
        for name in os.listdir(section_dir):
            workflow_dir = section_dir / name
            if (workflow_dir / "README.rst").exists():
                output_dir = srcdir / "examples" / section
                output_dir.mkdir(parents=True, exist_ok=True)
                workflow_dir = input_dir / section / name
                rel_workflow_dir = os.path.relpath(workflow_dir, output_dir)

                if (workflow_dir / "example.rst").exists():
                    template_file = f"{section}/{name}/example.rst"
                    print("using", f"{section}/{name}/example.rst")
                else:
                    template_file = "example.rst"
                template = jinja_env.get_template(template_file)
                text = template.render(abs_workflow_dir=workflow_dir, workflow_dir=rel_workflow_dir, os=os)

                rst_file = output_dir / f"{name}.rst"
                rst_file.write_text(text)
                logger.info(f"Generated {rst_file}")
                examples[section].append(name)

    # Generate index
    template = jinja_env.get_template("index.rst")
    text = template.render(examples=examples)
    rst_file = srcdir / "examples" / "index.rst"
    rst_file.write_text(text)


def setup(app):
    app.connect("builder-inited", genexamples)

    return {"version": "0.1"}
