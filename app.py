from flask import Flask, render_template, request, jsonify, Response
import pandas as pd
from datetime import datetime, timedelta, timezone
import requests

app = Flask(__name__)

def time_to_seconds(time_str):
    hh, mm, ss = map(int, time_str.split(':'))
    return hh * 3600 + mm * 60 + ss

def seconds_to_time(seconds):
    seconds = int(seconds)
    hh = seconds // 3600
    mm = (seconds % 3600) // 60
    ss = seconds % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"

def dias_desde_ultima_atualizacao(data_str):
    try:
        data_ultima_atualizacao = datetime.strptime(data_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        data_atual = datetime.now(timezone.utc)
        diferenca = data_atual - data_ultima_atualizacao.replace(tzinfo=timezone.utc)
        return min(max(0, diferenca.days), 120)
    except ValueError:
        return None

def carregar_e_preprocessar_api(url_api):
    try:
        response = requests.get(url_api)
        response.raise_for_status()
        dados = response.json()
        if "watchtimes" not in dados:
            return None
        df = pd.DataFrame(dados["watchtimes"])

        colunas_mapeadas = {
            'user_email': 'Email',
            'user_full_name': 'Nome Completo',
            'lesson_name': 'Aula',
            'course_name': 'Curso',
            'until_completed_duration': 'Duração',
            'updated_at': 'Última Atualização'
        }

        colunas_necessarias = list(colunas_mapeadas.keys())
        for col in colunas_necessarias:
            if col not in df.columns:
                return None

        df = df.rename(columns=colunas_mapeadas)

        def hhmmss_para_segundos(tempo):
            h, m, s = map(int, tempo.split(":"))
            return h * 3600 + m * 60 + s

        df['Duração'] = df['Duração'].apply(lambda x: hhmmss_para_segundos(x))
        df['dias_sem_acesso'] = df['Última Atualização'].apply(dias_desde_ultima_atualizacao)
        df['Última Atualização'] = pd.to_datetime(df['Última Atualização'], errors='coerce', utc=True)

        return df
    except Exception as e:
        print("Erro em carregar_e_preprocessar_api:", e)
        return None

def processar_tempo_por_aluno_e_aula(df):
    df_grouped = df.groupby(['Nome Completo', 'Email', 'Aula', 'Curso'], as_index=False).agg({
        'Duração': 'sum',
        'dias_sem_acesso': 'min',
        'Última Atualização': 'max'
    })
    df_grouped['Duração'] = df_grouped['Duração'].apply(seconds_to_time)
    return df_grouped

def aplicar_filtro(df, from_date=None, to_date=None, intervalo=None, cidade=None):
    df['Última Atualização'] = pd.to_datetime(df['Última Atualização'], errors='coerce', utc=True)

    if intervalo:
        agora = datetime.now(timezone.utc)
        if intervalo.endswith("m"):
            delta = timedelta(minutes=int(intervalo[:-1]))
        elif intervalo.endswith("h"):
            delta = timedelta(hours=int(intervalo[:-1]))
        elif intervalo.endswith("d"):
            delta = timedelta(days=int(intervalo[:-1]))
        else:
            delta = timedelta(0)
        inicio = agora - delta
        df = df[df['Última Atualização'] >= inicio]

    elif from_date and to_date:
        try:
            inicio = pd.to_datetime(from_date).tz_localize("UTC")
            fim = pd.to_datetime(to_date).tz_localize("UTC") + timedelta(days=1)
            df = df[(df['Última Atualização'] >= inicio) & (df['Última Atualização'] < fim)]
        except Exception as e:
            print("aplicar_filtro: erro ao parsear from/to:", e)

    if cidade and cidade != "todos":
        if cidade == "itabira":
            df = df[df['Email'].str.contains("@pditabira", na=False)]
        elif cidade == "bomdespacho":
            df = df[df['Email'].str.contains("@pdbomdespacho", na=False)]

    return df

def montar_url_api(from_date=None, to_date=None):
    agora = datetime.now(timezone.utc)
    inicio_mes = agora.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    if from_date and to_date:
        try:
            inicio = pd.to_datetime(from_date).tz_localize("UTC")
            fim = pd.to_datetime(to_date).tz_localize("UTC") + timedelta(days=1)
        except:
            inicio, fim = inicio_mes, agora
    else:
        inicio, fim = inicio_mes, agora

    inicio_iso = inicio.isoformat().replace("+00:00", "Z")
    fim_iso = fim.isoformat().replace("+00:00", "Z")

    return f"https://watchtime.projetodesenvolve.online/watchtime?fromCompleted={inicio_iso}&toCompleted={fim_iso}&fromUpdated={inicio_iso}&toUpdated={fim_iso}&ignoreStaff=true"

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/dados')
def dados_filtrados():
    from_date = request.args.get('from')   
    to_date = request.args.get('to')      
    intervalo = request.args.get('intervalo') 
    cidade = request.args.get('cidade')

    url_relatorio_api = montar_url_api(from_date, to_date)
    df_relatorio = carregar_e_preprocessar_api(url_relatorio_api)

    if df_relatorio is None or df_relatorio.empty:
        return jsonify([])

    df_relatorio = df_relatorio[df_relatorio['Email'].str.match(r'.*@(pditabira\.com|pdbomdespacho\.com\.br)$')]
    df_relatorio = aplicar_filtro(df_relatorio, from_date, to_date, intervalo, cidade)
    df_final = processar_tempo_por_aluno_e_aula(df_relatorio)

    return jsonify(df_final.to_dict(orient='records'))


from flask import Flask, request, Response
import pandas as pd
from datetime import datetime, timedelta
import re

from flask import Flask, request, Response
import pandas as pd
from datetime import datetime, timedelta
import re

@app.route('/exportar_csv')
def exportar_csv():
    intervalo = request.args.get('intervalo')  # exemplo: "15m", "3h", "2d"
    from_param = request.args.get('from')      # filtro manual inicial
    to_param = request.args.get('to')          # filtro manual final
    cidade = request.args.get('cidade')
    agora = datetime.now()

    # 🔹 Inicializa from_date e to_date
    from_date = to_date = agora

    # 🔹 Prioridade 1: intervalo relativo (15m, 3h, 2d)
    if intervalo:
        match = re.match(r"(\d+)([mhd])", intervalo)
        if match:
            quantidade = int(match.group(1))
            tipo = match.group(2)
            if tipo == "m":
                from_date = agora - timedelta(minutes=quantidade)
            elif tipo == "h":
                from_date = agora - timedelta(hours=quantidade)
            elif tipo == "d":
                from_date = agora - timedelta(days=quantidade)
            to_date = agora

    # 🔹 Prioridade 2: filtro manual de datas (só se intervalo relativo não foi passado)
    elif from_param or to_param:
        from_date = pd.to_datetime(from_param) if from_param else agora
        to_date = pd.to_datetime(to_param) if to_param else agora

    # 🔹 Converte datas para strings BR
    def formatar_data(data):
        return data.strftime("%d-%m-%Y") if data else None

    from_br = formatar_data(from_date)
    to_br = formatar_data(to_date)

    # 🔹 Nome do arquivo baseado nas datas efetivamente usadas
    if from_br == to_br:
        nome_arquivo = f"relatorio_watchtime_{from_br}.csv"
    else:
        nome_arquivo = f"relatorio_watchtime_{from_br}_ate_{to_br}.csv"

    # 🔹 Debug para ver o nome do arquivo gerado
    print(f"[DEBUG] Nome do arquivo gerado: {nome_arquivo}")

    # 🔹 Carrega e processa dados
    url_relatorio_api = montar_url_api(from_date, to_date)
    df_relatorio = carregar_e_preprocessar_api(url_relatorio_api)

    if df_relatorio is None or df_relatorio.empty:
        return Response("Nenhum dado para exportar", mimetype="text/plain")

    df_relatorio = df_relatorio[df_relatorio['Email'].str.match(r'.*@(pditabira\.com|pdbomdespacho\.com\.br)$')]
    df_relatorio = aplicar_filtro(df_relatorio, from_date, to_date, intervalo, cidade)
    df_final = processar_tempo_por_aluno_e_aula(df_relatorio)

    csv_data = df_final.to_csv(index=False, sep=";", encoding="utf-8-sig")

    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-disposition": f"attachment; filename={nome_arquivo}"}
    )



if __name__ == '__main__':
    app.run(debug=True)
