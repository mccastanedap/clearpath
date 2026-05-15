FROM public.ecr.aws/lambda/python:3.11

# Instalar compiladores necesarios
RUN yum install -y gcc gcc-c++ python3-devel

# Copiar requirements e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código del pipeline
COPY main.py .
COPY src/ src/
COPY clearpath_dbt/ clearpath_dbt/

# Handler de Lambda
CMD ["main.lambda_handler"]