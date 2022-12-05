FROM python:3.9
LABEL maintainer="Sammy Cui <sammycui98@gmail.com>"
ENV DOCKER=true
WORKDIR /fermi_backend
COPY ./requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

COPY fermi_backend /app/fermi_backend

EXPOSE 8888

CMD ["uvicorn", "fermi_backend.webapp.main:app", "--host", "0.0.0.0", "--port", "8888"]