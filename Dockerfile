FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_all_types_random.py random_person.py city_provider.py mapping.json staedte_komplett.csv ./

RUN useradd --system --no-create-home appuser
USER appuser

ENTRYPOINT ["python", "load_all_types_random.py"]
