from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sympy import symbols, diff, solve, N
import psycopg2
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST", "postgres") 
DB_NAME = os.getenv("POSTGRES_DB", "airflow")
DB_USER = os.getenv("POSTGRES_USER", "airflow")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "airflow")
DB_PORT = os.getenv("POSTGRES_PORT", "5432") 

DB_HOST = 'localhost'  
DB_PORT = '5433'     

app = FastAPI(title="API de Otimização da Qualidade do Ar")

origins = [
    "http://localhost",
    "http://127.0.0.1",
    "null",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class OtimizacaoRequest(BaseModel):
    cidade: str | None = None
    poluente_coluna: str

def get_db_conn():
    try:
        print('Host:', DB_HOST)
        print('Database:', DB_NAME)
        print('User:', DB_USER)
        print('Password:', DB_PASS)
        print('Port:', DB_PORT)

        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        raise HTTPException(status_code=500, detail="Falha na conexão com o banco de dados.")

def calcular_otimizacao(df_agregado: pd.DataFrame, grau_polinomial=4):
    """
    Executa a Regressão Polinomial e a Otimização Simbólica (SymPy).
    """
    if len(df_agregado) < grau_polinomial + 1:
        raise ValueError(f"Dados insuficientes para regressão de grau {grau_polinomial}.")
        
    X = df_agregado['hora'].values.reshape(-1, 1)
    Y = df_agregado['poluicao_media'].values

    poly = PolynomialFeatures(degree=grau_polinomial)
    X_poly = poly.fit_transform(X)
    
    model = LinearRegression()
    model.fit(X_poly, Y)
    
    coefs = model.coef_
    intercept = model.intercept_
    
    t = symbols('t')
    f_t = intercept
    
    for i in range(1, grau_polinomial + 1):
        f_t += coefs[i] * t**i

    # 4. Critério da Primeira Derivada (Pontos Críticos)
    f_prime = diff(f_t, t)
    
    pontos_criticos = solve(f_prime, t)
    
    pontos_reais = []
    for p in pontos_criticos:
        try:
            p_val = float(N(p)) 
            if 0 <= p_val <= 23:
                pontos_reais.append(p_val)
        except TypeError:
            continue

    logger.info(f"Pontos críticos (reais e no domínio [0, 23]): {pontos_reais}")

    # 5. Classificação (Critério da Segunda Derivada e Extremos)
    # Calcula a Segunda Derivada
    f_double_prime = diff(f_prime, t)
    
    candidatos = list(set(pontos_reais + [0.0, 23.0]))
    
    resultados_f = []
    
    for tc in candidatos:
        valor_f = float(N(f_t.subs(t, tc))) 
        # Avalia o valor da segunda derivada para classificação (opcional, mas bom para relatório)
        # Classificacao: > 0 (Mínimo local), < 0 (Máximo local), = 0 (Sela/Inflexão)
        valor_fpp = float(N(f_double_prime.subs(t, tc)))
        
        resultados_f.append({
            'hora': tc, 
            'poluicao': valor_f, 
            'classificacao': 'Min' if valor_fpp > 0 else 'Max/Extremo'
        })

    min_global = min(resultados_f, key=lambda x: x['poluicao'])

    return min_global['hora'], min_global['poluicao']


@app.post("/api/otimizar")
def otimizar_poluicao_endpoint(request: OtimizacaoRequest):
    
    cidade = request.cidade
    poluente = request.poluente_coluna
    
    logger.info(f"Requisição: Poluente={poluente}, Cidade={cidade if cidade else 'Global'}")

    if cidade:
        where_clause = f"WHERE cidade = '{cidade}'"
        unidade_nome = f"em {cidade}"
    else:
        where_clause = ""
        unidade_nome = "média global"

    query = f"""
        SELECT
            hora_int AS hora,
            AVG({poluente}) AS poluicao_media
        FROM
            qualidade_ar_historico
        {where_clause}
        GROUP BY
            hora_int
        ORDER BY
            hora_int;
    """

    conn = get_db_conn()
    try:
        df_agregado = pd.read_sql(query, conn)
    finally:
        conn.close()

    if df_agregado.empty:
        raise HTTPException(status_code=404, detail="Não há dados suficientes para esta seleção.")
    
    try:
        horario_otimo, nivel_minimo = calcular_otimizacao(df_agregado)
    except ValueError as e:
        logger.error(f"Erro de cálculo: {e}")
        raise HTTPException(status_code=500, detail="Erro no modelo de otimização. Verifique a qualidade dos dados.")

    unidade_medida = "µg/m³" 
    if 'co_monoxido' in poluente:
        unidade_medida = "µg/m³"

    return {
        "localidade": unidade_nome,
        "poluente_coluna": poluente,
        "horario_otimo": horario_otimo,
        "nivel_minimo": nivel_minimo,
        "unidade": unidade_medida
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
