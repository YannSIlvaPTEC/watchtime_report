from flask import Flask, render_template, request, jsonify, Response
import pandas as pd
from datetime import datetime, timedelta, timezone
import requests
import re

app = Flask(__name__)

# ---------------- Funções auxiliares ---------------- #

def time_to_seconds(time_str):
    hh, mm, ss = map(int, time_str.split(':'))
    return hh * 3600 + mm * 60 + ss

def seconds_to_time(seconds):
    hh = seconds // 3600
    mm = (seconds % 3600) // 60
    ss = seconds % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"

def dias_desde_ultima_atualizacao(data_str):
    try:
        data = datetime.strptime(data_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        delta = datetime.now(timezone.utc) - data.replace(tzinfo=timezone.utc)
        return min(max(0, delta.days), 120)
    except ValueError:
        return None

# ---------------- API e Processamento ---------------- #

def carregar_api_por_intervalo(url_api, intervalo_dias=7):
    """
    Busca dados da API em intervalos de dias para não travar o Render.
    Por padrão, busca 7 dias de cada vez.
    """
    agora = datetime.now(timezone.utc)
    inicio = agora - timedelta(days=intervalo_dias)
    df_total = pd.DataFrame()

    while inicio < agora:
        fim = min(inicio + timedelta(days=intervalo_dias), agora)
        url = f"{url_api}&fromCompleted={inicio.isoformat()}Z&toCompleted={fim.isoformat()}Z"
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            dados = resp.json()
            if "watchtimes" not in dados or not dados["watchtimes"]:
                inicio = fim
                continue
            df = pd.DataFrame(dados["watchtimes"])
            df_total = pd.concat([df_total, df], ignore_index=True)
        except Exception as e:
            print("Erro ao buscar dados da API:", e)
        inicio = fim
    return df_total if not df_total.empty else None

def preprocessar_df(df):
    colunas = {
        'user_email': 'Email',
        'user_full_name': 'Nome Completo',
        'lesson_name': 'Aula',
        'course_name': 'Curso',
        'until_completed_duration': 'Duração',
        'updated_at': 'Última Atualização'
    }
    for c in colunas.keys():
        if c not in df.columns:
            return None
    df = df.rename(columns=colunas)
    df['Duração'] = df['Duração'].apply(time_to_seconds)
    df['Última Atualização'] = pd.to_datetime(df['Última Atualização'], errors='coerce', utc=True)
    df['dias_sem_acesso'] = df['Última Atualização'].apply(lambda x: dias_desde_ultima_atualizacao(x.isoformat()))
    return df

def aplicar_filtro(df, from_date=None, to_date=None, intervalo=None, cidade=None):
    df['Última Atualização'] = pd.to_datetime(df['Última Atualização'], errors='coerce', utc=True)
    agora = datetime.now(timezone.utc)

    if intervalo:
        match = re.match(r"(\d+)([mhd])", intervalo)
        if match:
            qty, typ = int(match.group(1)), match.group(2)
            delta = timedelta(minutes=qty) if typ=="m" else timedelta(hours=qty) if typ=="h" else timedelta(days=qty)
            df = df[df['Última Atualização'] >= agora - delta]
    elif from_date and to_date:
        try:
            inicio = pd.to_datetime(from_date).tz_localize("UTC")
            fim = pd.to_datetime(to_date).tz_localize("UTC") + timedelta(days=1)
            df = df[(df['Última Atualização'] >= inicio) & (df['Última Atualização'] < fim)]
        except:
            pass

    if cidade and cidade != "todos":
        if cidade == "itabira":
            df = df[df['Email'].str.contains("@pditabira", na=False)]
        elif cidade == "bomdespacho":
            df = df[df['Email'].str.contains("@pdbomdespacho", na=False)]

    return df

def processar_tempo_por_aluno_e_aula(df):
    df_grouped = df.groupby(['Nome Completo', 'Email', 'Aula', 'Curso'], as_index=False).agg({
        'Duração':'sum', 'dias_sem_acesso':'min', 'Última Atualização':'max'
    })
    df_grouped['Duração'] = df_grouped['Duração'].apply(seconds_to_time)
    return df_grouped

# ---------------- Rotas ---------------- #

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/dados')
def dados_filtrados():
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    intervalo = request.args.get('intervalo')
    cidade = request.args.get('cidade')

    url_api = "https://watchtime.projetodesenvolve.online/watchtime?ignoreStaff=true"
    df = carregar_api_por_intervalo(url_api, intervalo_dias=7)
    if df is None:
        return jsonify([])

    df = preprocessar_df(df)
    df = aplicar_filtro(df, from_date, to_date, intervalo, cidade)
    df_final = processar_tempo_por_aluno_e_aula(df)
    return jsonify(df_final.to_dict(orient='records'))

@app.route('/exportar_csv')
def exportar_csv():
    from_date = request.args.get('from')
    to_date = request.args.get('to')
    intervalo = request.args.get('intervalo')
    cidade = request.args.get('cidade')

    url_api = "https://watchtime.projetodesenvolve.online/watchtime?ignoreStaff=true"
    df = carregar_api_por_intervalo(url_api, intervalo_dias=7)
    if df is None:
        return Response("Nenhum dado para exportar", mimetype="text/plain")

    df = preprocessar_df(df)
    df = aplicar_filtro(df, from_date, to_date, intervalo, cidade)
    df_final = processar_tempo_por_aluno_e_aula(df)

    # Nome do arquivo
    agora = datetime.now()
    from_br = pd.to_datetime(from_date).strftime("%d-%m-%Y") if from_date else agora.strftime("%d-%m-%Y")
    to_br = pd.to_datetime(to_date).strftime("%d-%m-%Y") if to_date else agora.strftime("%d-%m-%Y")
    nome_arquivo = f"relatorio_watchtime_{from_br}_ate_{to_br}.csv" if from_br != to_br else f"relatorio_watchtime_{from_br}.csv"

    csv_data = df_final.to_csv(index=False, sep=";", encoding="utf-8-sig")
    return Response(csv_data, mimetype="text/csv", headers={"Content-disposition": f"attachment; filename={nome_arquivo}"})

if __name__ == '__main__':
    app.run(debug=True)
