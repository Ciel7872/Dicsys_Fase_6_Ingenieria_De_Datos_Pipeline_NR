FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Copiar el código del pipeline y los módulos de configuración necesarios
COPY . ${WORKDIR}

# Instalar dependencias del template
RUN pip install -r ${WORKDIR}/dataflow/requirements.txt

# Establecer el módulo principal del template
ENV FLEX_TEMPLATE_PYTHON_PY_MODULE=dataflow.streaming_pipeline

# Comando por defecto
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]