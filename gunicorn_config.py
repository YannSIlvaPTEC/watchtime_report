# gunicorn_config.py

# Aumenta o timeout do worker do Gunicorn para 300 segundos (5 minutos).
# Isso é essencial para lidar com chamadas longas e síncronas de API 
# que processam grandes volumes de dados (como o Pandas) no Render.
# O valor padrão costuma ser 30 segundos, o que causa o erro de timeout (504).
timeout = 300

# Opcional: Define o número de workers.
# Em ambientes como o Render, é geralmente recomendado usar 1 ou 2 workers,
# mas o padrão costuma ser adequado para o plano gratuito/iniciante.
# workers = 2 
