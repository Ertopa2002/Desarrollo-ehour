
import oracledb
import boto3
import os
from datetime import datetime

# Variables de entorno esperadas
ORACLE_USER = os.environ['ORACLE_USER']
ORACLE_PASS = os.environ['ORACLE_PASS']
ORACLE_DSN = os.environ['ORACLE_DSN']
SES_REGION = os.environ['SES_REGION']
SES_FROM = os.environ['SES_FROM']
SES_TO = os.environ['SES_TO']

def get_data():
    with oracledb.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=ORACLE_DSN) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
              p.COD_PROYECTO,
              p.NOM_PROYECTO,
              p.DESC_PROYECTO,
              r.COD_USUARIO,
              SUM(r.HORASREPORTE) AS TOTAL_HORAS,
              SUM(r.HORASREPORTE * r.COSTOHORA) AS COSTO_TOTAL,
              COUNT(*) AS TOTAL_REGISTROS,
              MAX(r.FECHAREPORTE) AS ULTIMA_FECHAREPORTE
            FROM ALIGARE_USER.TAL_USUARIOREPORTE r
            JOIN ALIGARE_USER.TAL_PROYECTO p ON r.COD_PROYECTO = p.COD_PROYECTO
            WHERE r.COD_USUARIO IN ('esolano', 'ppavez', 'astrange', 'oromero', 'nsilva', 'salarcon')
              AND r.FECHAREPORTE >= TO_DATE('2025-05-01', 'YYYY-MM-DD')
              AND r.FECHAREPORTE < TO_DATE('2025-05-22', 'YYYY-MM-DD')
            GROUP BY p.COD_PROYECTO, p.NOM_PROYECTO, p.DESC_PROYECTO, r.COD_USUARIO
            ORDER BY p.COD_PROYECTO
        """)
        return cursor.fetchall()

def build_html(rows):
    html = "<h2>Reporte de Horas</h2><table border='1'><tr><th>Proyecto</th><th>Usuario</th><th>Horas</th><th>Costo</th><th>Fecha Última</th></tr>"
    for row in rows:
        html += f"<tr><td>{row[1]}</td><td>{row[3]}</td><td>{row[4]}</td><td>{row[5]:,.0f}</td><td>{row[7].strftime('%Y-%m-%d')}</td></tr>"
    html += "</table>"
    return html

def send_email(html):
    ses = boto3.client('ses', region_name=SES_REGION)
    response = ses.send_email(
        Source=SES_FROM,
        Destination={'ToAddresses': SES_TO.split(',')},
        Message={
            'Subject': {'Data': f"Reporte de Horas - {datetime.today().strftime('%Y-%m-%d')}"},
            'Body': {'Html': {'Data': html}}
        }
    )
    return response

def lambda_handler(event, context):
    try:
        rows = get_data()
        html = build_html(rows)
        send_email(html)
        return {'status': 'ok', 'rows_sent': len(rows)}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}
